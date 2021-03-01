package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.Utils
import org.example.Utils.time
import org.example.evaluators.{HashPoint, Hasher}

import scala.collection.{Iterable, mutable}

class KnnConstructionAlgorithm(val desiredSize: Int,
                               val baseDirectory: String)
    extends KnnConstruction {

    val min = desiredSize * Utils.MIN_TOLERANCE;
    val max = desiredSize * Utils.MAX_TOLERANCE;
    val radiusMultiplier = 1.2;

    override def build(data: RDD[(Long, Vector)]): Unit = {

        val dimension = data.first()._2.size;

        val (hasher, hashOptions, radius) = time(s"desiredSize = $desiredSize tolerance = ($min, $max) - ") {
            Hasher.getHasherForDataset(data, dimension, desiredSize)
        } // 2 min

        val numTables = hasher.numTables

        var currentData = data.flatMap { case (id, point) => (0 until numTables).map(tableIndex => (tableIndex, id, point)) }
        var currentRadius = radius
        var iteration = 0

        //val statistics = collection.mutable.Map[Int, Int]();

        var end = false;
        while (!end) {
            println(s"  R$iteration: $currentRadius")

            // Estadísticas locales
            val statistics = collection.mutable.Map[Int, Int]();

            // Condición para formar un bucket. Aquellos que no cumplan van a la siguiente ronda.
            val bucketCondition = (size: Int) => size >= min && size <= max

            time("    Se actualizan las estadísticas - ") {
                updateStatistics(currentData,
                    currentRadius, hasher,
                    bucketCondition,
                    statistics)
            }

            val remaining = getRemaining(currentData,
                currentRadius, hasher,
                !bucketCondition(_))

            /*val empty = time("    Comprobando si no hay puntos - ") {
                remaining.isEmpty()
            }*/
            /*val numPuntos = time("    Comprobando si no hay puntos - ") {
                remaining.count
            }
            val empty = numPuntos == 0*/
            val empty = time("    Comprobando si no hay puntos - ") { remaining.take(1).length == 0 }

            if (empty) {
                println("    Vacío!")

                end = true
            }
            else {
                val onlyBases = time("    Comprobando si todos los buckets son Base - ") {
                    Utils.forAll(remaining)({ case ((_, hash), _) => Utils.isBaseHashPoint(hash) })
                }

                currentData = time("    Calculando los puntos restantes - ") {
                    remaining.flatMap { case ((tableIndex, _), it) => it.map { case (id, point) => (tableIndex, id, point) } }
                }

                // DEBUG
                /*val numPuntos = time("    Calculando el número TOTAL de puntos para TODAS las tabla (DEBUG) - ") {
                    currentData.count
                }

                println(s"    Puntos restantes = $numPuntos")*/

                if (onlyBases) {
                    println("    Todos son BASE!")

                    time("    Se actualizan las estadísticas") {
                        updateStatistics(currentData,
                            currentRadius, hasher,
                            _ => true,
                            statistics)
                    }

                    end = true
                }
                else {
                    currentRadius = currentRadius * radiusMultiplier
                    iteration = iteration + 1

                    //currentTable = hashOptions.newHashEvaluator()
                }
            }

            println("    Estadísticas: Número de puntos - Número de buckets");
            showStatistics(statistics)
        }
    }

    def showStatistics(statistics: mutable.Map[Int, Int]): Unit = {
        time {
            statistics.toSeq.sortBy(_._1).foreach { case (numPoints, count) => println(s"      $numPoints - $count") }
        }
    }

    def groupByHasher(hashedData: RDD[(Int, Long, Vector)],
                      radius: Double, hasher: Hasher): RDD[((Int, HashPoint), Iterable[(Long, Vector)])] = {
        val bhasher = hashedData.sparkContext.broadcast(hasher)
        val grouped = hashedData.map { case (tableIndex, id, point) => ((tableIndex, bhasher.value.hash(tableIndex, point, radius)), (id, point)) }
            .groupByKey()
        grouped
    }

    def updateStatistics(hashedData: RDD[(Int, Long, Vector)],
                         radius: Double, hasher: Hasher,
                         sizeFilter: Int => Boolean,
                         statistics: mutable.Map[Int, Int]): Unit = {

        groupByHasher(hashedData, radius, hasher)
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

        groupByHasher(hashedData, radius, hasher)
            .filter { case (_, it) => sizeFilter(it.size) }
    }
}
