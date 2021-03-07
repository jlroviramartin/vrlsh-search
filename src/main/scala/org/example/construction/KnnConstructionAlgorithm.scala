package org.example.construction

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, HashOptions, LookupProvider, Utils}
import org.example.Utils.time
import org.example.evaluators.{Hash, HashPoint, Hasher}

import scala.collection.{Iterable, mutable}

class KnnConstructionAlgorithm(val desiredSize: Int,
                               val baseDirectory: String)
    extends KnnConstruction {

    val min = desiredSize * Utils.MIN_TOLERANCE;
    val max = desiredSize * Utils.MAX_TOLERANCE;
    val radiusMultiplier = 1.4;

    override def build(data: RDD[(Long, Vector)]): Unit = {

        val dimension = data.first()._2.size;

        //val (hasher, hashOptions, radius) = time(s"desiredSize = $desiredSize tolerance = ($min, $max)") {
        //    Hasher.getHasherForDataset(data, dimension, desiredSize)
        //} // 2 min
        val radius = 0.1
        val hasher = new HashOptions(dimension, 16, 14).newHasher()

        val sc = data.sparkContext
        var bhasher = sc.broadcast(hasher)
        var bsetToFilter: Broadcast[Set[(Hash)]] = null
        var bsetBases: Broadcast[Set[(Int)]] = null

        val lookupProvider = time("    Se hace broadcast de los datos") {
            new BroadcastLookupProvider(data)
        }

        var currentIndices = data.map { case (id, point) => id }
        var currentRadius = radius
        var iteration = 0

        // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
        var bucketCondition = (size: Int) => size >= min && size <= max

        while (currentIndices.take(1).length != 0) {
            println(s"  R$iteration: $currentRadius")
            println(s"  puntos restantess> ${currentIndices.count()}")

            // Estadísticas locales
            val statistics = collection.mutable.Map[Int, Int]();

            val currentData = currentIndices.flatMap(id => bhasher.value.hash(lookupProvider.lookup(id), currentRadius).map(hash => (hash, id)))

            if (time("    Se comprueba si todos son BASE") {
                Utils.forAll(currentData) { case (hash, _) => Utils.isBaseHashPoint(hash) }
            }) {
                println("    Todos son BASE");

                // En la siguiente iteración TODOS forman buckets
                bucketCondition = _ => true
            }

            val hashWithNumPoints = currentData
                .aggregateByKey(0)(
                    { case (numPoints, id) => numPoints + 1 },
                    (numPoints1, numPoints2) => numPoints1 + numPoints2)
                .filter { case (hash, numPoints) => bucketCondition(numPoints) }

            time("    Se actualizan las estadísticas") {
                hashWithNumPoints
                    .collect()
                    .foreach { case (hash, numPoints) => {
                        Utils.addOrUpdate(statistics, numPoints, 1, (prev: Int) => prev + 1)
                    }
                    }
            }

            println("    Estadísticas: Número de puntos - Número de buckets");
            showStatistics(statistics)

            val currentBuckets = time("    Se obtienen los buckets construidos") {
                hashWithNumPoints
                    .map { case (hash, numPoints) => hash }
                    .collect()
                    .toSet
            }

            val bcurrentBuckets = time("    Se hace broadcast de los buckets construidos") {
                sc.broadcast(currentBuckets)
            }

            val remaining = currentData.filter { case (hash, id) => !bcurrentBuckets.value.contains(hash) }

            val usedPoints = currentData.filter { case (hash, id) => bcurrentBuckets.value.contains(hash) }
                .map { case (hash, id) => id }
                .distinct()
                .collect()
                .toSet

            val busedPoints = time("    Se hace broadcast de los puntos usados") {
                sc.broadcast(usedPoints)
            }
            currentIndices = currentIndices.filter(id => !busedPoints.value.contains(id))

            currentRadius = currentRadius * radiusMultiplier
            iteration = iteration + 1

            /*if (Utils.forAll(remaining) { case (hash, _) => Utils.isBaseHashPoint(hash) }) {
                println("    Todos son BASE");

                // En la siguiente iteración TODOS forman buckets
                bucketCondition = _ => true
            } else {
                currentIndices = time("    Se obtienen los puntos restantes") {
                    currentData.filter { case (hash, id) => !bcurrentBuckets.value.contains(hash) }
                        .map { case (hash, id) => id }
                        .distinct()
                }

                currentRadius = currentRadius * radiusMultiplier
                iteration = iteration + 1
            }*/
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
