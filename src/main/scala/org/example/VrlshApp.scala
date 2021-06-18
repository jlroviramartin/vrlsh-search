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
import org.example.statistics.DefaultStatisticsCollector
import org.example.testing.TestingUtils.{doQueriesWithResult, time_doQueriesWithResult}

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import scala.collection._

// How to turn off INFO logging in Spark? https://stackoverflow.com/a/26123496
object VrlshApp {
    val GROUND_TRUTH_SIZE = 1000

    def main(args: Array[String]) {
        val spark = SparkUtils.initSpark("3")

        //val fos = new FileOutputStream(new File("C:/Temp/log.txt"))

        // Calculan los nuevos datasets
        val testOptions = new TestOptions()
        /*testOptions.datasets = Array(/*"siftsmall",*/ "audio" /*, "sift"*/)
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
        testOptions.ks = Array(100)*/

        testOptions.datasets = Array(/*"siftsmall", "corel", "shape", "audio",*/ "sift")
        testOptions.dataFilePath = immutable.Map(
            "corel" -> Paths.get("C:\\datasets\\corel\\corel_random_652317.csv"),
            "shape" -> Paths.get("C:\\datasets\\shape\\shape_random_27775.csv"),
            "siftsmall" -> Paths.get("C:\\datasets\\siftsmall\\siftsmall_base_random_9900.csv"),
            "audio" -> Paths.get("C:\\datasets\\audio\\audio_base_random_53387.csv"),
            "sift" -> Paths.get("C:\\datasets\\sift\\sift_base_random_990000.csv"))
        testOptions.testFilePath = immutable.Map(
            "corel" -> Paths.get("C:\\datasets\\corel\\corel_random_10000.csv"),
            "shape" -> Paths.get("C:\\datasets\\shape\\shape_random_1000.csv"),
            "siftsmall" -> Paths.get("C:\\datasets\\siftsmall\\siftsmall_base_random_100.csv"),
            "audio" -> Paths.get("C:\\datasets\\audio\\audio_base_random_1000.csv"),
            "sift" -> Paths.get("C:\\datasets\\sift\\sift_base_random_10000.csv"))
        testOptions.ts = Array(5, 10, 20, 40, 80)
        testOptions.ks = Array(100)
        //testOptions.samples = 1000

        val DIRECTORY = "C:/result/vrlsh-6"

        time(spark, testOptions, DIRECTORY)

        // Se guardan los hashers: dataset -> DIRECTORY/<name>/<desiredSize>/hasher.dat
        /*{
            println("=====> storeAllHashers =====")
            SparkUtils.updateCores(spark, "3")
            storeAllHashers(spark, testOptions, DIRECTORY)
        }*/

        // Se guardan los resultados: dataset -> DIRECTORY/<name>/<desiredSize>/KnnQuery.dat
        //                                       DIRECTORY/<name>/<desiredSize>/data/
        //                                       DIRECTORY/<name>/<desiredSize>/model/
        /*{
            println("=====> storeAll =====")
            SparkUtils.updateCores(spark, "3")
            storeAll(spark, testOptions, DIRECTORY)
        }*/

        //printVrlsh(spark, testOptions, DIRECTORY)

        // Se guardan las máximas distancias: testset -> DIRECTORY/<name>/max-distances.csv
        /*{
            println("=====> maxDistances =====")
            SparkUtils.updateCores(spark, "*")
            maxDistances(spark, testOptions, DIRECTORY)
        }*/

        // Se guardan los resultados reales: testset -> DIRECTORY/groundtruth/<name>/<size>/groundtruth.csv
        /*{
            println("=====> groundTruthAll =====")
            SparkUtils.updateCores(spark, "*")
            groundTruthAll(spark, testOptions, s"$DIRECTORY/groundtruth", GROUND_TRUTH_SIZE)
        }*/

        // Se evaluan los knn: testset -> DIRECTORY/result/<name>/<k>/result.csv
        //                                DIRECTORY/result/<name>/<k>/statistics.csv
        {
            println("=====> evaluations =====")
            SparkUtils.updateCores(spark, "*")
            evaluations(spark, testOptions, DIRECTORY)
        }

        // Se evalua el error: testset -> DIRECTORY/statistics/<name>/global-statistics.csv
        //                                DIRECTORY/statistics/<name>/quality-statistics.csv
        {
            println("=====> evaluateError =====")
            SparkUtils.updateCores(spark, "*")
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
                    VrlshKnnConstructionAlgorithm.createAndStore(data, hasherFactory, desiredSize, baseDirectory)
                }
            }
        }
    }

    def time(spark: SparkSession,
             testOptions: TestOptions,
             outputPath: String): Unit = {
        val sc = spark.sparkContext
        val distanceEvaluator = new KnnEuclideanSquareDistance

        val result = testOptions.datasets.flatMap { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val queries = testOptions.loadTestFileWithId(sc, name)
            val size = queries.size

            testOptions.ts.flatMap { t =>
                println(s"  === $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.map { k =>
                    println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                    val desiredSize = t * k

                    val baseDirectory = Paths.get(outputPath, s"$name/$desiredSize")

                    val constructionTime = {
                        val hasherFactory = new LoadHasherFactory(baseDirectory)

                        val begin = System.currentTimeMillis()
                        VrlshKnnConstructionAlgorithm.time_createAndStore(data, hasherFactory, desiredSize, baseDirectory)
                        System.currentTimeMillis() - begin
                    }

                    val queryTime = {
                        // Se construye/deserializa el objeto knnQuery
                        val knnQuery = VrlshKnnConstructionAlgorithm.load(sc, baseDirectory)

                        val begin2 = System.currentTimeMillis()
                        time_doQueriesWithResult(knnQuery, distanceEvaluator, k, queries.toList, new DefaultStatisticsCollector())
                        (System.currentTimeMillis() - begin2) / size.toDouble
                    }

                    (name, t, k, desiredSize, constructionTime, queryTime)
                }
            }
        }

        val names = testOptions.datasets.reduce((a, b) => a + "_" + b)
        val outputDirectory = Paths.get(outputPath, s"statistics/$names")

        CsvUtils.storeTimeResult(outputDirectory, result.to[immutable.Iterable])
    }

    def printVrlsh(spark: SparkSession,
                   testOptions: TestOptions,
                   outputPath: String): Unit = {
        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            testOptions.ts.foreach { t =>
                println(s"  === $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                    val desiredSize = t * k

                    val baseDirectory = Paths.get(outputPath, s"$name/$desiredSize")

                    val knnQuery = VrlshKnnConstructionAlgorithm.load(spark.sparkContext, baseDirectory)
                    knnQuery.printResume()
                }
            }
        }
    }

    /*def storeApproximateResult(spark: SparkSession,
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
    }*/

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
            val queries = testOptions.loadTestFileWithId(sc, name)

            println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

            val baseDirectory = Paths.get(inputPath, s"$name/$k")
            Files.createDirectories(baseDirectory)

            val distanceEvaluator = new KnnEuclideanSquareDistance
            val result = TestingUtils.doGroundTruth(data, distanceEvaluator, k, queries.toIterator)

            CsvUtils.storeGroundTruth(baseDirectory, result, k)
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

            val distanceEvaluator = new KnnEuclideanSquareDistance
            val result = TestingUtils.doMaxDistances(data, distanceEvaluator, queries.toIterator)

            CsvUtils.storeMaxDistances(baseDirectory, result)
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

            val distanceEvaluator = new KnnEuclideanSquareDistance

            testOptions.ts.foreach { t =>
                testOptions.ks.foreach { k =>
                    val desiredSize = t * k

                    val baseDirectory = Paths.get(inputPath, s"$name/$desiredSize")
                    val outputDirectory = Paths.get(inputPath, s"result/$name/$desiredSize")
                    Files.createDirectories(outputDirectory)

                    // Se construye/deserializa el objeto knnQuery
                    val knnQuery = VrlshKnnConstructionAlgorithm.load(sc, baseDirectory)

                    println("==== Evaluating =====")

                    val statistics = new DefaultStatisticsCollector()
                    val result = doQueriesWithResult(knnQuery, distanceEvaluator, k, queries.toList, statistics)

                    CsvUtils.storeApproximateResult(outputDirectory, result, k)
                    statistics.csv(outputDirectory.resolve("statistics.csv"))
                }
            }
        }
    }

    /*def time_evaluations(spark: SparkSession,
                         testOptions: TestOptions,
                         inputPath: String): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"## Dataset $name")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val queries = testOptions.loadTestFileWithId(sc, name)

            val distanceEvaluator = new KnnEuclideanSquareDistance

            testOptions.ts.foreach { t =>
                testOptions.ks.foreach { k =>
                    val desiredSize = t * k

                    val baseDirectory = Paths.get(inputPath, s"$name/$desiredSize")

                    // Se construye/deserializa el objeto knnQuery
                    val knnQuery = VrlshKnnConstructionAlgorithm.load(sc, baseDirectory)

                    println("==== Evaluating =====")

                    val statistics = new DefaultStatisticsCollector()
                    val begin = System.currentTimeMillis()
                    time_doQueriesWithResult(knnQuery, distanceEvaluator, k, queries.toList, statistics)
                    val milis = System.currentTimeMillis() - begin

                    println(s"    Evaluation time: ${milis}")
                }
            }
        }
    }*/

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

                    val gtSize = GROUND_TRUTH_SIZE
                    val gtFile = Paths.get(inputPath, s"groundtruth/$name/$gtSize/groundtruth.csv")
                    val groundTruth = SparkUtils.readGroundTruthFileByFilename(sc, gtFile.toString)

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
        CsvUtils.storeQuality(outputDirectory, quality)

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
        CsvUtils.storeGlobal(outputDirectory, gobal)
    }
}
