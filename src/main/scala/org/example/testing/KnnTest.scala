package org.example.testing

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.Utils.{MAX_TOLERANCE, MIN_TOLERANCE, RANDOM}
import org.example.construction.{DefaultStatisticsCollector, KnnConstructionAlgorithm}
import org.example.{EnvelopeDouble, KnnEuclideanDistance, KnnEuclideanSquareDistance}

import java.nio.file.Path
import org.example.testing.TestingUtils._

object KnnTest {
    def testSet_v1(envelope: EnvelopeDouble,
                   data: RDD[(Long, Vector)],
                   baseDirectory: Path,
                   k: Int,
                   t: Int = 5): Unit = {
        println(f"Min of axes: ${envelope.sizes.min}%1.5f")
        println()

        println(f"Max of axes: ${envelope.sizes.max}%1.5f")
        println()

        println(f"Max distance approx.: ${envelope.maxDistance}%1.5f")
        println()

        val count = data.count()
        val maxDistance = envelope.maxDistance

        val desiredSize = t * k
        val distanceEvaluator = new KnnEuclideanDistance

        // Se construye/deserializa el objeto knnQuery
        val knnQuery = KnnConstructionAlgorithm.createOrLoad(
            data,
            desiredSize,
            baseDirectory)

        val testExact = 50
        val testInside = 500
        val testOutside = 500
        val factorOutside = 0.5

        println("## Exact points")
        println()

        val exactQueries = data.takeSample(withReplacement = false, testExact, RANDOM.nextLong).map(x => x._2)
        val exactStatistics = new DefaultStatisticsCollector()
        val exactErrorCollector = new DefaultErrorCollector(count, maxDistance, k)
        doQueries(knnQuery, data, distanceEvaluator, k, exactQueries.toList, exactStatistics, exactErrorCollector)
        exactErrorCollector.showAverageOfErrors()

        println("## Inside points")
        println()

        // Se calculan n puntos aleatorios dentro del recubrimiento
        val insideQueries = randomInside(envelope, testInside)
        val insideStatistics = new DefaultStatisticsCollector()
        val insideErrorCollector = new DefaultErrorCollector(count, maxDistance, k)
        doQueries(knnQuery, data, distanceEvaluator, k, insideQueries.toList, insideStatistics, insideErrorCollector)
        insideErrorCollector.showAverageOfErrors()

        println("## Outside points")
        println()

        // Se calculan n puntos aleatorios dentro del recubrimiento
        val outsideQueries = randomOutside(envelope, factorOutside, testOutside)
        val outsideStatistics = new DefaultStatisticsCollector()
        val outsideErrorCollector = new DefaultErrorCollector(count, maxDistance, k)
        doQueries(knnQuery, data, distanceEvaluator, k, outsideQueries.toList, outsideStatistics, outsideErrorCollector)
        outsideErrorCollector.showAverageOfErrors()
    }

    def prepareData_v2(data: RDD[(Long, Vector)],
                       baseDirectory: Path,
                       k: Int,
                       t: Int = 5): Unit = {

        val desiredSize = t * k

        println(f"Desired size: $desiredSize")
        println()

        println("==== Building model =====")
        println()

        // Se construye/deserializa el objeto knnQuery
        val knnQuery = KnnConstructionAlgorithm.createOrLoad(
            data,
            desiredSize,
            baseDirectory)

        println("==== Buckets =====")
        println()

        knnQuery.printResume()
    }

    def testSet_v2(data: RDD[(Long, Vector)],
                   baseDirectory: Path,
                   outputDirectory: Path,
                   queries: Iterable[Vector],
                   k: Int,
                   t: Int = 5): Unit = {

        val envelope = data
            .map { case (_, point) => point }
            .aggregate(EnvelopeDouble.EMPTY)(
                EnvelopeDouble.seqOp,
                EnvelopeDouble.combOp)

        val distanceEvaluator = new KnnEuclideanSquareDistance

        val count = data.count()
        val maxDistance = envelope.maxDistance(distanceEvaluator)

        println(f"Min of axes: ${envelope.sizes.min}%1.5f")
        println()

        println(f"Max of axes: ${envelope.sizes.max}%1.5f")
        println()

        println(f"Max distance approx.: $maxDistance%1.5f")
        println()

        val desiredSize = t * k

        println(f"Desired size: $desiredSize [${MIN_TOLERANCE * desiredSize}, ${MAX_TOLERANCE * desiredSize}]")
        println()

        println("==== Building model =====")
        println()

        // Se construye/deserializa el objeto knnQuery
        val knnQuery = KnnConstructionAlgorithm.createOrLoad(
            data,
            desiredSize,
            baseDirectory)

        println("==== Buckets =====")
        println()

        knnQuery.printResume()

        println("==== Evaluating =====")
        println()

        val statistics = new DefaultStatisticsCollector()
        val errorCollector = new DefaultErrorCollector(count, maxDistance, k)

        doQueries(knnQuery, data, distanceEvaluator, k, queries.toList, statistics, errorCollector)

        statistics.csv(outputDirectory.resolve("statistics.csv"))
        errorCollector.csv(outputDirectory.resolve("error.csv"))

        errorCollector.showAverageOfErrors()
    }

}
