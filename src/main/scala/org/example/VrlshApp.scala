package org.example

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.construction.{VrlshKnnConstructionAlgorithm, VrlshKnnQuery}
import org.example.testing.{KnnTest, TestOptions, TestingUtils}

import java.nio.file.{Files, Path, Paths}
import java.util.{Calendar, Date}
import org.example.Utils.{MAX_TOLERANCE, MIN_TOLERANCE, RANDOM_SEED, time}
import org.example.evaluators.{Hasher, HasherFactory, LoadHasherFactory}

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import scala.collection._

// How to turn off INFO logging in Spark? https://stackoverflow.com/a/26123496
object VrlshApp {
    def main(args: Array[String]) {

        //val fos = new FileOutputStream(new File("C:/Temp/log.txt"))
        val fos = System.out

        val spark = SparkUtils.initSpark()

        /*
                val format = new SimpleDateFormat("d-M-y H:m:s")

                val start = Calendar.getInstance().getTime()
                println(start)

                val distanceEvaluator = new KnnEuclideanSquareDistance

                val test = SparkUtils.readDataFileByFilename(spark.sparkContext, "C:\\datasets\\gist\\gist_query.csv")
                val query = test.first()._2

                val knnResult = SparkUtils.readDataFileByFilename(spark.sparkContext, "C:\\datasets\\gist\\gist_base.csv")
                    .map { case (id, v) => (distanceEvaluator.distance(query, v), id) }
                    .aggregate(new KnnResult())(KnnResult.seqOp(50), KnnResult.combOp(50))

                val end = Calendar.getInstance().getTime()
                println(end)
                println(TimeUnit.MILLISECONDS.toSeconds(end.getTime - start.getTime))
        */

        // Calculan los nuevos datasets
        Console.withOut(fos) {
            val testOptions = new TestOptions()
            testOptions.datasets = Array(/*"siftsmall",*/ "audio"/*, "sift"*/)
            //testOptions.trainRatio = 0.999 // 1000 testing
            testOptions.dataFilePath = immutable.Map(
                "siftsmall" -> Paths.get("C:\\datasets\\siftsmall\\siftsmall_base.csv"),
                "audio" -> Paths.get("C:\\datasets\\audio\\audio_base.csv"),
                "gist" -> Paths.get("C:\\datasets\\gist\\gist_base.csv"),
                "sift" -> Paths.get("C:\\datasets\\sift\\sift_base.csv"))
            testOptions.testFilePath = immutable.Map(
                "siftsmall" -> Paths.get("C:\\datasets\\siftsmall\\siftsmall_query.csv"),
                "audio" -> Paths.get("C:\\datasets\\audio\\audio_query.csv"),
                "gist" -> Paths.get("C:\\datasets\\gist\\gist_query.csv"),
                "sift" -> Paths.get("C:\\datasets\\sift\\sift_query.csv"))
            testOptions.ts = Array(5, 10, 20, 40, 80)
            testOptions.ks = Array(100)

            val DIRECTORY = "C:/result/vrlsh-fufu"

            // Se guardan los hashers: dataset -> DIRECTORY/<name>/<desiredSize>/hasher.dat
            println("=====> storeAllHashers =====")
            storeAllHashers(spark, testOptions, DIRECTORY)

            // Se guardan los resultados: dataset -> DIRECTORY/<name>/<desiredSize>/KnnQuery.dat
            //                                       DIRECTORY/<name>/<desiredSize>/data/
            //                                       DIRECTORY/<name>/<desiredSize>/model/
            println("=====> storeAll =====")
            storeAll(spark, testOptions, DIRECTORY)

            // Se guardan las máximas distancias: testset -> DIRECTORY/<name>/max-distances.csv
            println("=====> maxDistances =====")
            //maxDistances(spark, testOptions, DIRECTORY)

            // Se guardan los resultados reales: testset -> DIRECTORY/groundtruth/<name>/<size>/groundtruth.csv
            println("=====> groundTruthAll =====")
            //groundTruthAll(spark, testOptions, s"$DIRECTORY/groundtruth", 1000)

            // Se evaluan los knn: testset -> DIRECTORY/result/<name>/<k>/result.csv
            //                                DIRECTORY/result/<name>/<k>/statistics.csv
            println("=====> evaluations =====")
            evaluations(spark, testOptions, DIRECTORY)

            // Se evalua el error: testset -> DIRECTORY/statistics/<name>/global-statistics.csv
            //                                DIRECTORY/statistics/<name>/quality-statistics.csv
            println("=====> evaluateError =====")
            evaluateError(spark, testOptions, DIRECTORY)
        }

        spark.stop()
    }

    def storeAllHashers(spark: SparkSession,
                        testOptions: TestOptions,
                        outputPath: String): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            testOptions.ts.foreach { t =>
                println(s"  === $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                    val desiredSize = t * k
                    val dimension = data.first()._2.size

                    val baseDirectory = Paths.get(outputPath, s"$name", s"$desiredSize")
                    Files.createDirectories(baseDirectory)

                    LoadHasherFactory.store(baseDirectory, data, desiredSize)
                }
            }
        }
    }

    def storeAll(spark: SparkSession,
                 testOptions: TestOptions,
                 outputPath: String): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            testOptions.ts.foreach { t =>
                println(s"  === $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                    val desiredSize = t * k

                    val baseDirectory = Paths.get(outputPath, s"$name/$desiredSize")
                    Files.createDirectories(baseDirectory)

                    val hasherFactory = new LoadHasherFactory(baseDirectory)

                    KnnTest.createAndStore(data, hasherFactory, baseDirectory, k, t)
                }
            }
        }
    }

    def storeApproximateResult(spark: SparkSession,
                               testOptions: TestOptions,
                               inputPath: String): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val testing = testOptions.loadTestFile(sc, name)

            testOptions.ts.foreach { t =>
                println(s"  === $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                    val desiredSize = t * k

                    val baseDirectory = Paths.get(inputPath, s"$name/$desiredSize")
                    Files.createDirectories(baseDirectory)

                    KnnTest.testSet_v2(data, baseDirectory, baseDirectory, testing.to[immutable.Iterable], k)
                }
            }
        }
    }

    def groundTruthAll(spark: SparkSession,
                       testOptions: TestOptions,
                       inputPath: String,
                       k: Integer): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val testing = testOptions.loadTestFileWithId(sc, name)

            println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

            val baseDirectory = Paths.get(inputPath, s"$name/$k")
            Files.createDirectories(baseDirectory)

            KnnTest.storeGroundTruth(data, baseDirectory, testing.to[immutable.Iterable], k)
        }
    }

    def maxDistances(spark: SparkSession,
                     testOptions: TestOptions,
                     inputPath: String): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val queries = testOptions.loadTestFileWithId(sc, name)

            println(s"    ${new Date(System.currentTimeMillis())} =====")

            val baseDirectory = Paths.get(inputPath, s"$name")
            Files.createDirectories(baseDirectory)

            KnnTest.storeMaxDistances(data, baseDirectory, queries.to[immutable.Iterable])
        }
    }

    def evaluations(spark: SparkSession,
                    testOptions: TestOptions,
                    inputPath: String): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"## Dataset $name")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val queries = testOptions.loadTestFileWithId(sc, name)

            val envelope = data
                .map { case (_, point) => point }
                .aggregate(EnvelopeDouble.EMPTY)(
                    EnvelopeDouble.seqOp,
                    EnvelopeDouble.combOp)

            val distanceEvaluator = new KnnEuclideanSquareDistance

            val count = data.count()
            val maxDistance = envelope.maxDistance(distanceEvaluator)

            testOptions.ts.foreach { t =>
                testOptions.ks.foreach { k =>
                    val desiredSize = t * k

                    val baseDirectory = Paths.get(inputPath, s"$name/$desiredSize")
                    val outputDirectory = Paths.get(inputPath, s"result/$name/$desiredSize")
                    Files.createDirectories(outputDirectory)

                    KnnTest.storeApproximateResult(sc,
                        baseDirectory,
                        outputDirectory,
                        queries.to[immutable.Iterable],
                        k)
                }
            }
        }
    }

    def evaluateError(spark: SparkSession,
                      testOptions: TestOptions,
                      inputPath: String): Unit = {
        val sc = spark.sparkContext

        val names = testOptions.datasets.reduce((a, b) => a + "_" + b)
        val outputDirectory = Paths.get(inputPath, s"statistics/$names")

        println("Quality statistics")

        val quality = testOptions.datasets.flatMap { name =>
            println(s"  Dataset $name")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val queries = testOptions.loadTestFileWithId(sc, name)

            testOptions.ts.flatMap { t =>
                testOptions.ks.flatMap { k =>
                    val desiredSize = t * k

                    // Se construye/deserializa el objeto knnQuery
                    val baseDirectory = Paths.get(inputPath, s"$name/$desiredSize")

                    val knnQuery = VrlshKnnConstructionAlgorithm.load(data.sparkContext, baseDirectory)

                    val gtSize = 1000
                    val gt = Paths.get(inputPath, s"groundtruth/$name/$gtSize/groundtruth.csv")
                    val groundTruth = SparkUtils.readGroundTruthFileByFilename(sc, gt.toString)

                    val appoxFile = Paths.get(inputPath, s"result/$name/$desiredSize/result.csv")
                    val approximateResult = SparkUtils.readApproxFileByFilename(sc, appoxFile.toString)

                    val maxFile = Paths.get(inputPath, s"$name/max-distances.csv")
                    val maxDistances = SparkUtils.readMaxDistancesFileByFilename(sc, maxFile.toString)

                    val distanceEvaluator = new KnnEuclideanSquareDistance

                    val statistics = TestingUtils.checkError(data,
                        sc.parallelize(queries),
                        groundTruth,
                        approximateResult,
                        maxDistances,
                        distanceEvaluator,
                        k)
                        .map(s => {
                            s.dataset = name
                            s.t = t
                            s.k = k
                            s
                        })

                    statistics.collect()
                }
            }
        }
        KnnTest.storeQuality(quality, outputDirectory)

        println("Global statistics")

        val gobal = testOptions.datasets.flatMap { name =>
            println(s"    Dataset $name")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            testOptions.ts.flatMap { t =>
                testOptions.ks.map { k =>
                    val desiredSize = t * k

                    // Se construye/deserializa el objeto knnQuery
                    val baseDirectory = Paths.get(inputPath, s"$name/$desiredSize")

                    val knnQuery = VrlshKnnConstructionAlgorithm.load(data.sparkContext, baseDirectory)

                    val generalStatistics = knnQuery.getGeneralStatistics()
                    generalStatistics.dataset = name
                    generalStatistics.t = t
                    generalStatistics.k = k

                    generalStatistics
                }
            }
        }
        KnnTest.storeGlobal(gobal, outputDirectory)
    }
}
