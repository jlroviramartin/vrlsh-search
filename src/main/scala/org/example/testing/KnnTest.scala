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

    def createAndStore(data: RDD[(Long, Vector)],
                       hasherFactory: HasherFactory,
                       baseDirectory: Path,
                       k: Int,
                       t: Int = 5): Unit = {

        val desiredSize = t * k

        println(f"Desired size: $desiredSize")
        println()

        println("==== Building model =====")
        println()

        // Se construye/deserializa el objeto knnQuery
        val knnQuery = VrlshKnnConstructionAlgorithm.createAndStore(data, hasherFactory, desiredSize, baseDirectory)

        println("==== Buckets =====")
        println()

        //knnQuery.printResume()
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

    def storeApproximateResult(sc: SparkContext,
                               baseDirectory: Path,
                               outputDirectory: Path,
                               queries: immutable.Iterable[(Long, Vector)],
                               k: Int): Unit = {

        val distanceEvaluator = new KnnEuclideanSquareDistance

        println("==== Building model =====")

        // Se construye/deserializa el objeto knnQuery
        val knnQuery = VrlshKnnConstructionAlgorithm.load(sc, baseDirectory)

        println("==== Evaluating =====")

        val statistics = new DefaultStatisticsCollector()

        val result = doQueriesWithResult(knnQuery, distanceEvaluator, k, queries.toList, statistics)
        if (result.nonEmpty) {
            val header = Array("id") ++ (0 until k).map(i => "k" + i)

            val settings = new CsvWriterSettings
            settings.setHeaders(header: _*)
            settings.setHeaderWritingEnabled(true)
            settings.getFormat.setDelimiter(",")
            settings.getFormat.setLineSeparator("\n")

            val csvWriter = new CsvWriter(outputDirectory.resolve("result.csv").toFile, StandardCharsets.UTF_8, settings)

            var index = 0

            result.foreach { case (id, approx) => {
                if (index % 100 == 0) {
                    println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                val row = Array(Long.box(id)) ++
                    (0 until approx.length).map(i => Long.box(approx(i))) ++
                    (approx.length until k).map(i => Long.box(-1))

                csvWriter.writeRow(row: _*)

                //csvWriter.writeRow(id, approx)
            }
            }
            csvWriter.close()
        }

        statistics.csv(outputDirectory.resolve("statistics.csv"))
    }

    /*def testSet_v3(data: RDD[(Long, Vector)],
                   baseDirectory: Path,
                   outputDirectory: Path,
                   queries: immutable.Iterable[(Long, Vector)],
                   k: Int): Unit = {

        val sc = data.sparkContext

        val distanceEvaluator = new KnnEuclideanSquareDistance

        println("==== Loading model =====")
        println()

        // Se construye/deserializa el objeto knnQuery
        val knnQuery = VrlshKnnConstructionAlgorithm.load(sc, baseDirectory)

        println("==== Resume =====")
        println()

        knnQuery.printResume()

        println("==== Evaluating =====")
        println()

        val statistics = new DefaultStatisticsCollector()

        doFastQueries(knnQuery, distanceEvaluator, k, queries, statistics)

        statistics.csv(outputDirectory.resolve("statistics.csv"))
    }*/

    def storeGroundTruth(data: RDD[(Long, Vector)],
                         outputDirectory: Path,
                         queries: immutable.Iterable[(Long, Vector)],
                         k: Int): Unit = {

        val distanceEvaluator = new KnnEuclideanSquareDistance

        println("==== Writing csv =====")

        val file = outputDirectory.resolve(s"groundtruth.csv")

        val header = Array("id") ++ (0 until k).map(i => "k" + i)

        val settings = new CsvWriterSettings
        settings.setHeaders(header: _*)
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0

        // Knn real
        TestingUtils.doGroundTruth(data, distanceEvaluator, k, queries.toIterator)
            .foreach { case (queryId, array) => {
                if (index % 100 == 0) {
                    println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                val row = Array(Long.box(queryId)) ++ (0 until k).map(i => Long.box(array(i)))
                csvWriter.writeRow(row: _*)
            }
            }

        csvWriter.close()
    }

    def storeMaxDistances(data: RDD[(Long, Vector)],
                          outputDirectory: Path,
                          queries: immutable.Iterable[(Long, Vector)]): Unit = {

        val distanceEvaluator = new KnnEuclideanSquareDistance

        println("==== Writing csv =====")

        val file = outputDirectory.resolve("max-distances.csv")

        val settings = new CsvWriterSettings
        settings.setHeaders("id", "maxDistance")
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0
        TestingUtils.doMaxDistances(data, distanceEvaluator, queries.toIterator)
            .foreach { case (id, maxDistance) => {
                if (index % 100 == 0) {
                    println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                csvWriter.writeRow(Long.box(id), Double.box(maxDistance))
            }
            }

        csvWriter.close()
    }

    def storeGlobal(data: Array[GeneralStatistics],
                    outputDirectory: Path): Unit = {

        println("==== Writing csv =====")

        val file = outputDirectory.resolve("global-statistics.csv")

        val settings = new CsvWriterSettings
        settings.setHeaders("dataset", "t", "k", "desiredSize", "numTables", "keyLength", "dimension", "ratioOfPoints", "totalNumBuckets", "totalNumPoints", "totalNumLevels")
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0
        data
            .foreach(statistics => {
                if (index % 100 == 0) {
                    println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                csvWriter.writeRow(
                    statistics.dataset,
                    Int.box(statistics.t),
                    Int.box(statistics.k),
                    Int.box(statistics.desiredSize),
                    Int.box(statistics.numTables),
                    Int.box(statistics.keyLength),
                    Int.box(statistics.dimension),
                    Double.box(statistics.ratioOfPoints),
                    Int.box(statistics.totalNumBuckets),
                    Int.box(statistics.totalNumPoints),
                    Int.box(statistics.totalNumLevels))
            })

        csvWriter.close()
    }

    def storeQuality(data: Array[QualityStatistics],
                     outputDirectory: Path): Unit = {

        println("==== Writing csv =====")

        val file = outputDirectory.resolve("quality-statistics.csv")

        val settings = new CsvWriterSettings
        settings.setHeaders("dataset", "t", "k", "desiredSize", "avgIndexError", "avgDistanceErrorNorm", "avgDistanceError", "avgPrecision", "recall", "apk")
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0
        data
            .foreach(statistics => {
                if (index % 100 == 0) {
                    println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                csvWriter.writeRow(
                    statistics.dataset,
                    Int.box(statistics.t),
                    Int.box(statistics.k),
                    Int.box(statistics.desiredSize),
                    Double.box(statistics.avgIndexError),
                    Double.box(statistics.avgDistanceErrorNorm),
                    Double.box(statistics.avgDistanceError),
                    Double.box(statistics.avgPrecision),
                    Double.box(statistics.recall),
                    Double.box(statistics.apk)
                )
            })

        csvWriter.close()
    }
}
