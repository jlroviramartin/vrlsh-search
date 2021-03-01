package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.Utils.time
import org.example.buckets.Bucket
import org.example.evaluators.{HashEvaluator, HashPoint, Hasher}
import org.example.{HashOptions, Utils}

import java.nio.file.Path
import scala.collection.{Iterable, mutable}

class KnnConstructionAlgorithmOLD(val desiredSize: Int,
                                  val baseDirectory: String)
    extends KnnConstruction {

    val min = desiredSize * Utils.MIN_TOLERANCE;
    val max = desiredSize * Utils.MAX_TOLERANCE;
    val radiusMultiplier = 1.2;

    override def build(data: RDD[(Long, Vector)]): Unit = {

        val dimension = data.first()._2.size;
        println(s"desiredSize = $desiredSize tolerance = ($min, $max)")

        val (hasher, hashOptions, radius) = time {
            Hasher.getHasherForDataset(data, dimension, desiredSize)
        } // 3:45 min

        var numTable = 1;
        for (table <- hasher.tables) {
            println(s"Tabla: $numTable")
            build(data, radius, table, hashOptions);

            numTable = numTable + 1
        }
    }

    def build(data: RDD[(Long, Vector)],
              radius: Double,
              table: HashEvaluator,
              hashOptions: HashOptions): Unit = {

        var currentTable = table

        var currentData = data
        var currentRadius = radius
        var iteration = 0

        val statistics = collection.mutable.Map[Int, Int]();

        var end = false;
        while (!end) {
            println(s"Iteración : $iteration")

            /*println("    Almacena los buckets")
                storeBuckets(currentData,
                    currentRadius, table,
                    (size: Int) => size >= min && size <= max,
                    baseDirectory)*/

            println("    Estadísticas");
            updateStatistics(currentData,
                currentRadius, currentTable,
                (size: Int) => size >= min && size <= max,
                statistics);

            val remaining = getRemaining(currentData,
                currentRadius, currentTable,
                (size: Int) => size < min || size > max)

            if (remaining.isEmpty()) {
                println("    Vacio!")

                end = true
            }
            else if (Utils.forAll(remaining)({ case (hash, _) => Utils.isBaseHashPoint(hash) })) {
                println("    Todos son BASE!")

                val remainingData = remaining.flatMap { case (_, it) => it }

                // DEBUG
                println(s"    Puntos restantes = ${remainingData.count}")

                updateStatistics(remainingData,
                    currentRadius, currentTable,
                    _ => true,
                    statistics)

                end = true
            }
            else {
                currentData = remaining.flatMap { case (_, it) => it }
                currentRadius = currentRadius * radiusMultiplier
                iteration = iteration + 1

                //currentTable = hashOptions.newHashEvaluator()

                // DEBUG
                println(s"    Puntos restantes = ${currentData.count}")
            }
        }

        println("    Estadísticas: Número de puntos - Número de buckets");
        statistics.toSeq.sortBy(_._1).foreach { case (numPoints, count) => {
            println(s"$numPoints - $count")
        }
        }
    }

    def updateStatistics(data: RDD[(Long, Vector)],
                         radius: Double, table: HashEvaluator,
                         sizeFilter: Int => Boolean,
                         statistics: mutable.Map[Int, Int]): Unit = {
        val btable = data.sparkContext.broadcast(table)
        val grouped = data.map({ case (id, point) => (btable.value.hash(point, radius), (id, point)) })
            .groupByKey

        grouped
            .filter { case (_, it) => sizeFilter(it.size) }
            .map { case (_, it) => (it.size, 1) }
            .reduceByKey((count1, count2) => count1 + count2) // Se cuentan los buckets con el mismo número de puntos
            .collect()
            .foreach { case (numPoints, count) =>
                Utils.addOrUpdate(statistics, numPoints, count, (prev: Int) => prev + count)
            }
    }

    def storeBuckets(data: RDD[(Long, Vector)],
                     radius: Double, table: HashEvaluator,
                     sizeFilter: Int => Boolean,
                     baseDirectory: Path): Unit = {
        val btable = data.sparkContext.broadcast(table)
        val grouped = data.map({ case (id, point) => (btable.value.hash(point, radius), (id, point)) })
            .groupByKey

        grouped
            .filter { case (_, it) => sizeFilter(it.size) }
            .map { case (hash, it) => (hash, new Bucket(it.map { case (_, point) => point })) }
            .foreach { case (hash, bucket) => bucket.store(radius, hash, baseDirectory) }
    }

    def getRemaining(data: RDD[(Long, Vector)],
                     radius: Double, table: HashEvaluator,
                     sizeFilter: Int => Boolean): RDD[(HashPoint, Iterable[(Long, Vector)])] = {
        val btable = data.sparkContext.broadcast(table)
        val grouped = data.map({ case (id, point) => (btable.value.hash(point, radius), (id, point)) })
            .groupByKey

        val remaining = grouped
            .filter { case (_, it) => sizeFilter(it.size) }
        remaining
    }
}
