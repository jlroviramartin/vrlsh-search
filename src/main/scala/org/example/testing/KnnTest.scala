package org.example.testing

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.Utils.{MAX_TOLERANCE, MIN_TOLERANCE, RANDOM_SEED}
import org.example.construction.{VrlshKnnConstructionAlgorithm, VrlshKnnQuery}
import org.example.evaluators.HasherFactory
import org.example.statistics.{DefaultStatisticsCollector, GeneralStatistics, QualityStatistics}
import org.example.{EnvelopeDouble, KnnDistance, KnnEuclideanDistance, KnnEuclideanSquareDistance, KnnResult}

import java.nio.file.Path
import org.example.testing.TestingUtils._

import java.nio.charset.StandardCharsets
import scala.collection._

object KnnTest {
/*
    def testSet_v1(envelope: EnvelopeDouble,
                   data: RDD[(Long, Vector)],
                   baseDirectory: Path,
                   k: Int,
                   t: Int = 5): Unit = {
        val sc = data.sparkContext

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
        val knnQuery = VrlshKnnConstructionAlgorithm.load(sc, baseDirectory)

        val testExact = 50
        val testInside = 500
        val testOutside = 500
        val factorOutside = 0.5

        println("## Exact points")
        println()

        val exactQueries = data.takeSample(withReplacement = false, testExact, RANDOM_SEED).map(x => x._2)
        val exactStatistics = new DefaultStatisticsCollector()
        val exactErrorCollector = new DefaultErrorCollector(count, k)
        doQueries(knnQuery, data, distanceEvaluator, k, exactQueries.toList, exactStatistics, exactErrorCollector)
        exactErrorCollector.showAverageOfErrors()

        println("## Inside points")
        println()

        // Se calculan n puntos aleatorios dentro del recubrimiento
        val insideQueries = randomInside(envelope, testInside)
        val insideStatistics = new DefaultStatisticsCollector()
        val insideErrorCollector = new DefaultErrorCollector(count, k)
        doQueries(knnQuery, data, distanceEvaluator, k, insideQueries.toList, insideStatistics, insideErrorCollector)
        insideErrorCollector.showAverageOfErrors()

        println("## Outside points")
        println()

        // Se calculan n puntos aleatorios dentro del recubrimiento
        val outsideQueries = randomOutside(envelope, factorOutside, testOutside)
        val outsideStatistics = new DefaultStatisticsCollector()
        val outsideErrorCollector = new DefaultErrorCollector(count, k)
        doQueries(knnQuery, data, distanceEvaluator, k, outsideQueries.toList, outsideStatistics, outsideErrorCollector)
        outsideErrorCollector.showAverageOfErrors()
    }

    def testSet_v2(data: RDD[(Long, Vector)],
                   baseDirectory: Path,
                   outputDirectory: Path,
                   queries: immutable.Iterable[Vector],
                   k: Int): Unit = {

        val sc = data.sparkContext

        val distanceEvaluator = new KnnEuclideanSquareDistance

        val count = data.count()

        println("==== Building model =====")

        // Se construye/deserializa el objeto knnQuery
        val knnQuery = VrlshKnnConstructionAlgorithm.load(sc, baseDirectory)

        println("==== Evaluating =====")

        val statistics = new DefaultStatisticsCollector()
        val errorCollector = new DefaultErrorCollector(count, k)

        doQueries(knnQuery, data, distanceEvaluator, k, queries.toList, statistics, errorCollector)

        statistics.csv(outputDirectory.resolve("statistics.csv"))
        errorCollector.csv(outputDirectory.resolve("error.csv"))

        errorCollector.showAverageOfErrors()
    }
*/
}
