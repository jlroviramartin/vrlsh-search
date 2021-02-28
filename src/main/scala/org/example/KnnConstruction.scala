package org.example

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.SimpleApp.{algorithmv2, getRemaining, updateStatistics}
import org.example.Utils.time
import org.example.evaluators.{HashEvaluator, HashPoint, Hasher}

import java.nio.file.Path
import scala.collection.{Iterable, mutable}

trait KnnConstruction extends Serializable {
    def build(data: RDD[(Long, Vector)])
}

class KnnConstructionAlgorithm1(val desiredSize: Int,
                                val baseDirectory: Path)
    extends KnnConstruction {

    val min = desiredSize * Utils.MIN_TOLERANCE;
    val max = desiredSize * Utils.MAX_TOLERANCE;

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
                currentRadius = currentRadius * 1.2
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


}


class KnnConstructionAlgorithm2(val desiredSize: Int,
                                val baseDirectory: Path)
    extends KnnConstruction {

    val min = desiredSize * Utils.MIN_TOLERANCE;
    val max = desiredSize * Utils.MAX_TOLERANCE;

    override def build(data: RDD[(Long, Vector)]): Unit = {

        val dimension = data.first()._2.size;
        println(s"desiredSize = $desiredSize tolerance = ($min, $max)")
        val (hasher, hashOptions, radius) = time {
            Hasher.getHasherForDataset(data, dimension, desiredSize)
        } // 3:45 min

        val numTables = hasher.numTables
        val hashedData = data.flatMap { case (id, point) => (0 until numTables).map(index => (index, id, point)) }

        var currentData = hashedData
        var currentRadius = radius
        var iteration = 0

        val statistics = collection.mutable.Map[Int, Int]();

        var end = false;
        while (!end) {
            println(s"Iteración : $iteration")

            println("    Estadísticas");
            updateStatistics(currentData,
                currentRadius, hasher,
                (size: Int) => size >= min && size <= max,
                statistics);

            val remaining = getRemaining(currentData,
                currentRadius, hasher,
                (size: Int) => size < min || size > max)

            if (remaining.isEmpty()) {
                println("    Vacio!")

                end = true
            }
            else if (Utils.forAll(remaining)({ case ((_, hash), _) => Utils.isBaseHashPoint(hash) })) {
                println("    Todos son BASE!")

                val remainingData = remaining.flatMap { case ((index, _), it) => it.map { case (id, point) => (index, id, point) } }

                // DEBUG
                println(s"    Puntos restantes = ${remainingData.count}")

                updateStatistics(remainingData,
                    currentRadius, hasher,
                    _ => true,
                    statistics)

                end = true
            }
            else {
                currentData = remaining.flatMap { case ((index, _), it) => it.map { case (id, point) => (index, id, point) } }
                currentRadius = currentRadius * 1.2
                iteration = iteration + 1

                //currentTable = hashOptions.newHashEvaluator()

                // DEBUG
                println(s"    Puntos restantes = ${currentData.count}")
            }
        }

        def updateStatistics(hashedData: RDD[(Int, Long, Vector)],
                             radius: Double, hasher: Hasher,
                             sizeFilter: Int => Boolean,
                             statistics: mutable.Map[Int, Int]): Unit = {
            val bhasher = data.sparkContext.broadcast(hasher)
            val grouped = hashedData.map { case (index, id, point) => ((index, bhasher.value.hash(index, point, radius)), (id, point)) }
                .groupByKey()

            grouped
                .filter { case (_, it) => sizeFilter(it.size) }
                .map { case (_, it) => (it.size, 1) }
                .reduceByKey((count1, count2) => count1 + count2) // Se cuentan los buckets con el mismo número de puntos
                .collect()
                .foreach { case (numPoints, count) =>
                    Utils.addOrUpdate(statistics, numPoints, count, (prev: Int) => prev + count)
                }
        }


        def getRemaining(hashedData: RDD[(Int, Long, Vector)],
                         radius: Double, hasher: Hasher,
                         sizeFilter: Int => Boolean): RDD[((Int, HashPoint), Iterable[(Long, Vector)])] = {

            val bhasher = data.sparkContext.broadcast(hasher)
            val grouped = hashedData.map { case (index, id, point) => ((index, bhasher.value.hash(index, point, radius)), (id, point)) }
                .groupByKey()

            val remaining = grouped
                .filter { case (_, it) => sizeFilter(it.size) }
            remaining
        }


    }
}