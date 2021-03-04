package org.example.construction

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.Utils
import org.example.Utils.time
import org.example.evaluators.{Hash, HashPoint, Hasher}

import scala.collection.{Iterable, mutable}

class KnnConstructionAlgorithm(val desiredSize: Int,
                               val baseDirectory: String)
    extends KnnConstruction {

    val min = desiredSize * Utils.MIN_TOLERANCE;
    val max = desiredSize * Utils.MAX_TOLERANCE;
    val radiusMultiplier = 1.2;

    override def build(data: RDD[(Long, Vector)]): Unit = {

        val dimension = data.first()._2.size;

        val (hasher, hashOptions, radius) = time(s"desiredSize = $desiredSize tolerance = ($min, $max)") {
            Hasher.getHasherForDataset(data, dimension, desiredSize)
        } // 2 min

        val sc = data.sparkContext
        var bhasher = sc.broadcast(hasher)
        var bsetToFilter: Broadcast[Set[(Hash)]] = null
        var bsetBases: Broadcast[Set[(Int)]] = null

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

            // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
            val bucketCondition = (size: Int) => size >= min && size <= max

            val setToFilter = time("    Se calculan los hash") {
                filterHashes(currentData,
                    radius,
                    bhasher,
                    bucketCondition)
            }

            //if (bsetToFilter != null) bsetToFilter.destroy()
            bsetToFilter = sc.broadcast(setToFilter)

            time("    Se actualizan las estadísticas") {
                updateStatistics(currentData,
                    currentRadius,
                    bhasher,
                    bsetToFilter,
                    statistics)
            }

            val remaining = time("    getRemaining") {
                getRemaining(currentData,
                    currentRadius,
                    bhasher,
                    bsetToFilter)
            }

            val setBases = time("    findBases") {
                findBases(remaining)
            }

            if (!setBases.isEmpty) {
                //if (bsetBases != null) bsetBases.destroy()
                bsetBases = sc.broadcast(setBases)

                time("    Se actualizan las estadísticas para las bases") {
                    updateStatisticsForBases(remaining.map { case ((numTable, hash), (id, point)) => (numTable, id, point) },
                        radius,
                        bhasher,
                        bsetBases,
                        statistics)
                }

                currentData = time("    Se eliminan las bases") {
                    removeBases(remaining.map { case ((numTable, hash), (id, point)) => (numTable, id, point) }, bsetBases)
                }
            } else {
                currentData = remaining.map { case ((numTable, hash), (id, point)) => (numTable, id, point) }
            }

            val empty = time("    Comprobando si no hay puntos") {
                currentData.take(1).length == 0
            }

            if (empty) {
                println("    Vacío!")

                end = true
            }
            else {
                currentRadius = currentRadius * radiusMultiplier
                iteration = iteration + 1

                //currentTable = hashOptions.newHashEvaluator()
            }

            println("    Estadísticas: Número de puntos - Número de buckets");
            showStatistics(statistics)
        }

        if (bsetToFilter != null) bsetToFilter.destroy()
        if (bsetBases != null) bsetBases.destroy()
        if (bhasher != null) bhasher.destroy()
    }

    def showStatistics(statistics: mutable.Map[Int, Int]): Unit = {
        time {
            statistics.toSeq.sortBy(_._1).foreach { case (numPoints, count) => println(s"      $numPoints - $count") }
        }
    }

    def sizesByHash(hashedData: RDD[(Int, Long, Vector)],
                    radius: Double,
                    bhasher: Broadcast[Hasher]): RDD[(Hash, Int)] = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .aggregateByKey(0)( // Se cuenta el número de puntos en cada bucket
                { case (numPts, id) => numPts + 1 },
                { case (numPts1, numPts2) => numPts1 + numPts2 })
    }

    def filterHashes(hashedData: RDD[(Int, Long, Vector)],
                     radius: Double,
                     bhasher: Broadcast[Hasher],
                     sizeFilter: Int => Boolean): Set[Hash] = {

        val set = sizesByHash(hashedData, radius, bhasher)
            .filter { case (_, size) => sizeFilter(size) }
            .map { case (hash, _) => hash }
            .collect().toSet
        set
    }

    def updateStatistics(hashedData: RDD[(Int, Long, Vector)],
                         radius: Double,
                         bhasher: Broadcast[Hasher],
                         bsetToFilter: Broadcast[Set[Hash]],
                         statistics: mutable.Map[Int, Int]): Unit = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .filter { case (hash, (_, _)) => bsetToFilter.value.contains(hash) }
            .aggregateByKey(0)(
                { case (numPoints, (id, point)) => numPoints + 1 },
                (numPoints1, numPoints2) => numPoints1 + numPoints2)
            .collect()
            .foreach { case (hash, numPoints) =>
                Utils.addOrUpdate(statistics, numPoints, 1, (prev: Int) => prev + 1)
            }
    }

    def updateStatisticsWithoutFilter(hashedData: RDD[(Int, Long, Vector)],
                                      radius: Double,
                                      bhasher: Broadcast[Hasher],
                                      statistics: mutable.Map[Int, Int]): Unit = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .aggregateByKey(0)(
                { case (numPoints, (id, point)) => numPoints + 1 },
                (numPoints1, numPoints2) => numPoints1 + numPoints2)
            .collect()
            .foreach { case (hash, numPoints) =>
                Utils.addOrUpdate(statistics, numPoints, 1, (prev: Int) => prev + 1)
            }
    }

    def updateStatisticsForBases(hashedData: RDD[(Int, Long, Vector)],
                                 radius: Double,
                                 bhasher: Broadcast[Hasher],
                                 bsetBases: Broadcast[Set[Int]],
                                 statistics: mutable.Map[Int, Int]): Unit = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .filter { case (hash, (_, _)) => bsetBases.value.contains(HashPoint.getIndex(hash)) }
            .aggregateByKey(0)(
                { case (numPoints, (id, point)) => numPoints + 1 },
                (numPoints1, numPoints2) => numPoints1 + numPoints2)
            .collect()
            .foreach { case (hash, numPoints) =>
                Utils.addOrUpdate(statistics, numPoints, 1, (prev: Int) => prev + 1)
            }
    }

    def removeBases(data: RDD[(Int, Long, Vector)],
                    bsetBases: Broadcast[Set[Int]]): RDD[(Int, Long, Vector)] = {

        data.filter { case (numTable, id, point) => !bsetBases.value.contains(numTable) }
    }

    def getRemaining(hashedData: RDD[(Int, Long, Vector)],
                     radius: Double,
                     bhasher: Broadcast[Hasher],
                     bsetToFilter: Broadcast[Set[Hash]]): RDD[((Int, Hash), (Long, Vector))] = {

        hashedData.map { case (tableIndex, id, point) => ((tableIndex, bhasher.value.hash(tableIndex, point, radius)), (id, point)) }
            .filter { case ((tableIndex, hash), (id, point)) => !bsetToFilter.value.contains(hash) }
    }

    def findBases(data: RDD[((Int, Hash), (Long, Vector))]): Set[Int] = {
        data.map { case ((numTable, hash), (id, point)) => (numTable, hash) }
            .aggregateByKey(true)( // Se comprueba si todos son base
                { case (isBase, hash) => isBase && Utils.isBaseHashPoint(hash) },
                { case (isBase1, isBase2) => isBase1 && isBase2 })
            .filter { case (numTable, isBase) => isBase }
            .map { case (numTable, isBase) => numTable }
            .distinct()
            .collect()
            .toSet
    }
}
