package org.example.lsh

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, HashOptions, KnnDistance, KnnResult}
import org.example.Utils.{RANDOM_SEED, time}
import org.example.construction.{KnnConstruction, KnnQuery, KnnQuerySerializable}
import org.example.evaluators.{Hash, Hasher}
import org.example.statistics.{EvaluationStatistics, StatisticsCollector}

import scala.collection.immutable.Iterable
import scala.util.Random

class LshKnnConstructionAlgorithm(val hasher: Hasher, val radius: Double)
    extends KnnConstruction {

    override def build(data: RDD[(Long, Vector)]): KnnQuery = {

        val lookupProvider = new BroadcastLookupProvider(data)

        // Resultado
        val result = time("Iteración") {
            val currentData = data
                .flatMap { case (id, point) => hasher.hash(point, radius).map(hash => (hash, id)) }
                .cache()

            // Se calculan los buckets (hash) y el número de puntos en cada uno
            val hashWithNumPoints = currentData
                .aggregateByKey(0)(
                    { case (numPoints, _) => numPoints + 1 },
                    (numPoints1, numPoints2) => numPoints1 + numPoints2)
                .cache()

            // Se muestran estadísticas: cuantos buckets tienen un cierto número de puntos
            showStatistics(hashWithNumPoints)
            showBucketCount(hashWithNumPoints)

            // Se calculan los datos que se van a usar para los buckets
            // Se actualiza el resultado
            currentData
                .aggregateByKey(Array[Long]())(
                    { case (array, id) => Array(id) ++ array },
                    { case (array1, array2) => array1 ++ array2 }
                )
                .map { case (hash, ids) => (hash, ids) }
        }

        println("Se finaliza!")

        new MyKnnQuery(hasher, radius, lookupProvider, result)
    }

    private def showStatistics(hashWithNumPoints: RDD[(Hash, Int)]): Unit = {
        println("    Estadísticas: Número de puntos - Número de buckets")
        time("    Se actualizan las estadísticas") {
            hashWithNumPoints
                .map { case (_, numPoints) => (numPoints, 1) }
                .reduceByKey((count1, count2) => count1 + count2)
                .sortByKey()
                .foreach { case (numPoints, count) => println(s"      $numPoints - $count") }
        }
    }

    private def showBucketCount(hashWithNumPoints: RDD[(Hash, Int)]): Unit = {
        val count = time("    Se cuenta el número de buckets creados") {
            hashWithNumPoints
                .count()
        }
        println(s"      Total buckets: $count")
    }
}

object LshKnnConstructionAlgorithm {
    def buildForSize(desiredSize: Int, data: RDD[(Long, Vector)]): LshKnnConstructionAlgorithm = {
        val dimension = data.first()._2.size
        val (hasher, hashOptions, radius) = Hasher.getHasherForDataset(data, dimension, desiredSize)
        new LshKnnConstructionAlgorithm(hasher, radius)
    }

    def build(dim: Int, radius: Double, numTables: Int, keyLength: Int): LshKnnConstructionAlgorithm = {
        val hashOptions = new HashOptions(new Random(RANDOM_SEED), dim, keyLength, numTables)
        val hasher = hashOptions.newHasher()
        new LshKnnConstructionAlgorithm(hasher, radius)
    }

    def buildForNormalizeData(dim: Int, numTables: Int, count: Long): LshKnnConstructionAlgorithm = {
        val radius = Math.pow(count, -1.0 / dim);
        val hashOptions = new HashOptions(new Random(RANDOM_SEED), dim, dim, numTables)
        val hasher = hashOptions.newHasher()
        new LshKnnConstructionAlgorithm(hasher, radius)
    }
}

class MyKnnQuery(val hasher: Hasher,
                 val radius: Double,
                 val lookupProvider: BroadcastLookupProvider,
                 val result: RDD[(Hash, Array[Long])])
    extends KnnQuery {

    def query(query: Vector,
              k: Int,
              distanceEvaluator: KnnDistance,
              statistics: StatisticsCollector): Iterable[(Double, Long)] = {
        val lookupProvider = this.lookupProvider

        val sc = this.result.sparkContext

        // Radios en los que se han encontrado puntos (son parte de la solución)
        val radiusesInResult: List[Double] = List()

        // Número de niveles recorridos
        val numLevels = 0

        val hashesSet = hasher
            .hash(query, radius)
            .toSet

        val bhashesSet = sc.broadcast(hashesSet)
        val bdistanceEvaluator = sc.broadcast(distanceEvaluator)

        val knnResult = time("Calculando los buckets") {
            result
                .filter { case (hash, ids) => bhashesSet.value.contains(hash) }
                .map { case (hash, ids) => ids.map(id => (bdistanceEvaluator.value.distance(query, lookupProvider.lookup(id)), id)) }
                .aggregate(new KnnResult())(
                    KnnResult.seqOpOfArray(k),
                    KnnResult.combOp(k))
        }

        time("statistics") {
            statistics.collect(
                new EvaluationStatistics(knnResult.size,
                    knnResult.comparisons,
                    knnResult.buckets,
                    numLevels,
                    radiusesInResult))
        }

        knnResult.sorted
    }

    def getSerializable(): KnnQuerySerializable = null

    def printResume(): Unit = {}
}

