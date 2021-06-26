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
        val spark = SparkUtils.initSpark("*")

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

        testOptions.datasets = Array(/*"siftsmall",*/ "corel", "shape", "audio", "sift")
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
        testOptions.ts = Array(5, 10, 20, 40/*, 80*/)
        testOptions.ks = Array(100)
        testOptions.samples = 1000

        val DATASET_PATH = "W:/spark/datasets"

        // Se guardan las máximas distancias: testset -> DATASET_PATH/max-distances/<name>/max-distances.csv
        /*{
            SparkUtils.updateCores(spark, "*")
            maxDistances(spark, testOptions,
                s"DATASET_PATH/max-distances)
        }*/

        // Se guardan los resultados reales: testset -> DATASET_PATH/groundtruth/<name>/<size>/groundtruth.csv
        /*{
            SparkUtils.updateCores(spark, "*")
            groundTruthAll(spark, testOptions,
                GROUND_TRUTH_SIZE,
                s"DATASET_PATH/groundtruth")
        }*/

        // Se guardan los hashers: dataset -> DATASET_PATH/hashers/<name>/<desiredSize>/hasher.dat
        /*{
            SparkUtils.updateCores(spark, "3")
            storeAllHashers(spark, testOptions,
                s"DATASET_PATH/hashers")
        }*/

        // ===== INICIO =====

        //prepareComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1)
        //prepareComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2)

        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 1)
        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 2)
        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 5)
        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 10)

        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 1)
        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 2)
        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 5)
        //evaluateComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 10)

        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 1)
        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 2)
        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 5)
        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 10)

        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 1)
        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 2)
        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 5)
        //evaluateErrorComparisonPerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 10)

        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 1)
        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 2)
        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 5)
        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 1, 10)

        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 1)
        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 2)
        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 5)
        //evaluateTimePerLevel(spark, testOptions, "C:/result/vrlsh-11", 2, 10)




        val RUTA_BASE = "W:/spark/result/vrlsh-13"
        val minLevels = 4

        println("''''' 1")
        prepareComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels)
/*
        println("''''' 2")
        evaluateComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 1)
        evaluateComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 2)
        evaluateComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 5)
        //evaluateComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 10)

        println("''''' 3")
        evaluateErrorComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 1)
        evaluateErrorComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 2)
        evaluateErrorComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 5)
        //evaluateErrorComparisonPerLevel(spark, testOptions, RUTA_BASE, minLevels, 10)

        println("''''' 4")
        //evaluateTimePerLevel(spark, testOptions, RUTA_BASE, minLevels, 1)
        //evaluateTimePerLevel(spark, testOptions, RUTA_BASE, minLevels, 2)
        //evaluateTimePerLevel(spark, testOptions, RUTA_BASE, minLevels, 5)
        //evaluateTimePerLevel(spark, testOptions, RUTA_BASE, minLevels, 10)
*/
        println("Finaliza: parada spark")

        spark.stop()
    }

    /*
    def testComparisonPerBucket(spark: SparkSession,
                                testOptions: TestOptions,
                                basePath: String,
                                defaultMinBuckets: Int,
                                defaultSearchInLevels: Int): Unit = {
        val DATASET_PATH = "C:/datasets"

        SparkUtils.updateCores(spark, "*")

        ////////////////////////////////////////////////////////////////////////////////////////////
        // Se construyen las estructuras las mediciones

        // 1 iteración

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 1
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 0

            val minBuckets = VrlshKnnConstructionAlgorithm.defaultMinBuckets
            val RESULT_PATH = s"$basePath-minBuckets-$minBuckets"

            storeAll(spark, testOptions,
                s"$DATASET_PATH/hashers",
                s"$RESULT_PATH")
        }

        // 30 iteraciones

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = defaultMinBuckets
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 0

            val minBuckets = VrlshKnnConstructionAlgorithm.defaultMinBuckets
            val RESULT_PATH = s"$basePath-minBuckets-$minBuckets"

            storeAll(spark, testOptions,
                s"$DATASET_PATH/hashers",
                s"$RESULT_PATH")
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // Se realizan las mediciones

        // 1 iteración y búsqueda en 1 nivel

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 1
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 0
            VrlshKnnQuery.searchInLevels = 1

            val minBuckets = VrlshKnnConstructionAlgorithm.defaultMinBuckets
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minBuckets-$minBuckets"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }

        // 1 iteración y búsqueda en 10 niveles

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 1
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 0
            VrlshKnnQuery.searchInLevels = defaultSearchInLevels

            val minBuckets = VrlshKnnConstructionAlgorithm.defaultMinBuckets
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minBuckets-$minBuckets"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }

        // 30 iteraciones y búsqueda en 1 nivel

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = defaultMinBuckets
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 0
            VrlshKnnQuery.searchInLevels = 1

            val minBuckets = VrlshKnnConstructionAlgorithm.defaultMinBuckets
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minBuckets-$minBuckets"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }

        // 30 iteraciones y búsqueda en 10 niveles

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = defaultMinBuckets
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 0
            VrlshKnnQuery.searchInLevels = defaultSearchInLevels

            val minBuckets = VrlshKnnConstructionAlgorithm.defaultMinBuckets
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minBuckets-$minBuckets"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }
    }

    def testComparisonPerLevel(spark: SparkSession,
                               testOptions: TestOptions,
                               basePath: String,
                               defaultMinLevels: Int,
                               defaultSearchInLevels: Int): Unit = {
        val DATASET_PATH = "C:/datasets"

        SparkUtils.updateCores(spark, "*")

        ////////////////////////////////////////////////////////////////////////////////////////////
        // Se construyen las estructuras las mediciones

        // 1 nivel

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 1

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            storeAll(spark, testOptions,
                s"$DATASET_PATH/hashers",
                s"$RESULT_PATH")
        }

        // n niveles

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = defaultMinLevels

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            storeAll(spark, testOptions,
                s"$DATASET_PATH/hashers",
                s"$RESULT_PATH")
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // Se realizan las mediciones

        // 1 iteración y búsqueda en 1 nivel

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 1
            VrlshKnnQuery.searchInLevels = 1

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }

        // 1 iteración y búsqueda en m niveles

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 1
            VrlshKnnQuery.searchInLevels = defaultSearchInLevels

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }

        // n iteraciones y búsqueda en 1 nivel

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = defaultMinLevels
            VrlshKnnQuery.searchInLevels = 1

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }

        // n iteraciones y búsqueda en m niveles

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = defaultMinLevels
            VrlshKnnQuery.searchInLevels = defaultSearchInLevels

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            evaluate(spark, testOptions,
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateRecallAndMap(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
        }
    }

    def testComparisonPerLevel_v2(spark: SparkSession,
                                  testOptions: TestOptions,
                                  basePath: String,
                                  defaultMinLevels: Int,
                                  defaultSearchInLevels: Int): Unit = {
        val DATASET_PATH = "C:/datasets"

        SparkUtils.updateCores(spark, "*")

        ////////////////////////////////////////////////////////////////////////////////////////////
        // Se realizan las mediciones

        // 1 iteración y búsqueda en 1 nivel

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 1
            VrlshKnnQuery.searchInLevels = 1

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateError(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")
        }

        // 1 iteración y búsqueda en m niveles

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = 1
            VrlshKnnQuery.searchInLevels = defaultSearchInLevels

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateError(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")
        }

        // n iteraciones y búsqueda en 1 nivel

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = defaultMinLevels
            VrlshKnnQuery.searchInLevels = 1

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateError(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")
        }

        // n iteraciones y búsqueda en m niveles

        {
            VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
            VrlshKnnConstructionAlgorithm.defaultMinLevels = defaultMinLevels
            VrlshKnnQuery.searchInLevels = defaultSearchInLevels

            val minLevels = VrlshKnnConstructionAlgorithm.defaultMinLevels
            val searchInLevels = VrlshKnnQuery.searchInLevels
            val RESULT_PATH = s"$basePath-minLevels-$minLevels"

            println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

            evaluateError(spark, testOptions,
                s"$DATASET_PATH/groundtruth",
                s"$DATASET_PATH/max-distances",
                s"$RESULT_PATH",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/result",
                s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")
        }
    }
*/

    def prepareComparisonPerLevel(spark: SparkSession,
                                  testOptions: TestOptions,
                                  basePath: String,
                                  minLevels: Int): Unit = {
        val DATASET_PATH = "C:/datasets"

        SparkUtils.updateCores(spark, "*")

        VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
        VrlshKnnConstructionAlgorithm.defaultMinLevels = minLevels

        val RESULT_PATH = s"$basePath-minLevels-$minLevels"

        storeAll(spark, testOptions,
            s"$DATASET_PATH/hashers",
            s"$RESULT_PATH")
    }

    def evaluateComparisonPerLevel(spark: SparkSession,
                                   testOptions: TestOptions,
                                   basePath: String,
                                   minLevels: Int,
                                   searchInLevels: Int): Unit = {
        val DATASET_PATH = "C:/datasets"

        SparkUtils.updateCores(spark, "*")

        VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
        VrlshKnnConstructionAlgorithm.defaultMinLevels = minLevels
        VrlshKnnQuery.searchInLevels = searchInLevels

        val RESULT_PATH = s"$basePath-minLevels-$minLevels"

        println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

        evaluate(spark, testOptions,
            s"$RESULT_PATH",
            s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

        //evaluateRecallAndMap(spark, testOptions,
        //    s"$DATASET_PATH/groundtruth",
        //    s"$DATASET_PATH/max-distances",
        //    s"$RESULT_PATH",
        //    s"$RESULT_PATH-searchInLevels-$searchInLevels/result")
    }

    def evaluateErrorComparisonPerLevel(spark: SparkSession,
                                        testOptions: TestOptions,
                                        basePath: String,
                                        minLevels: Int,
                                        searchInLevels: Int): Unit = {
        val DATASET_PATH = "C:/datasets"

        SparkUtils.updateCores(spark, "*")

        VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
        VrlshKnnConstructionAlgorithm.defaultMinLevels = minLevels
        VrlshKnnQuery.searchInLevels = searchInLevels

        val RESULT_PATH = s"$basePath-minLevels-$minLevels"

        println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

        evaluateError(spark, testOptions,
            s"$DATASET_PATH/groundtruth",
            s"$DATASET_PATH/max-distances",
            s"$RESULT_PATH",
            s"$RESULT_PATH-searchInLevels-$searchInLevels/result",
            s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")

        //time(spark,
        //    testOptions,
        //    s"$DATASET_PATH/hashers",
        //    s"$RESULT_PATH",
        //    s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")
    }

    def evaluateTimePerLevel(spark: SparkSession,
                             testOptions: TestOptions,
                             basePath: String,
                             minLevels: Int,
                             searchInLevels: Int): Unit = {
        val DATASET_PATH = "C:/datasets"

        SparkUtils.updateCores(spark, "*")

        VrlshKnnConstructionAlgorithm.defaultMinBuckets = 0
        VrlshKnnConstructionAlgorithm.defaultMinLevels = minLevels
        VrlshKnnQuery.searchInLevels = searchInLevels

        val RESULT_PATH = s"$basePath-minLevels-$minLevels"

        println(s"minLevels-$minLevels-searchInLevels-$searchInLevels")

        time(spark,
            testOptions,
            s"$DATASET_PATH/hashers",
            s"$RESULT_PATH",
            s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")
    }

    def test1(spark: SparkSession, testOptions: TestOptions, basePath: String): Unit = {
        val DATASET_PATH = "C:/datasets"

        VrlshKnnConstructionAlgorithm.defaultMinBuckets = 1
        VrlshKnnConstructionAlgorithm.defaultMinLevels = 0
        VrlshKnnQuery.searchInLevels = 1

        val minBuckets = VrlshKnnConstructionAlgorithm.defaultMinBuckets
        val searchInLevels = VrlshKnnQuery.searchInLevels
        val RESULT_PATH = s"$basePath-minBuckets-$minBuckets"

        SparkUtils.updateCores(spark, "*")

        // Se guardan los resultados: dataset -> RESULT_PATH/<name>/<desiredSize>/KnnQuery.dat
        //                                       RESULT_PATH/<name>/<desiredSize>/data/
        //                                       RESULT_PATH/<name>/<desiredSize>/model/
        storeAll(spark, testOptions,
            s"$DATASET_PATH/hashers",
            s"$RESULT_PATH")

        // Se evaluan los knn: testset -> RESULT_PATH/result/<name>/<k>/result.csv
        //                                RESULT_PATH/result/<name>/<k>/statistics.csv
        evaluate(spark, testOptions,
            s"$RESULT_PATH",
            s"$RESULT_PATH-searchInLevels-$searchInLevels/result")

        // Se evalua el error: testset -> RESULT_PATH/statistics/<name>/global-statistics.csv
        //                                RESULT_PATH/statistics/<name>/quality-statistics.csv
        evaluateError(spark, testOptions,
            s"$DATASET_PATH/groundtruth",
            s"$DATASET_PATH/max-distances",
            s"$RESULT_PATH",
            s"$RESULT_PATH-searchInLevels-$searchInLevels/result",
            s"$RESULT_PATH-searchInLevels-$searchInLevels/statistics")

        printVrlsh(spark, testOptions, s"$RESULT_PATH")

        //time(spark, testOptions, RESULT_PATH)
    }

    def storeAllHashers(spark: SparkSession,
                        testOptions: TestOptions,
                        outputPath: String): Unit = {
        val sc = spark.sparkContext

        println("=====> storeAllHashers =====")
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
                 hashersPath: String,
                 outputPath: String): Unit = {
        val sc = spark.sparkContext

        println("=====> storeAll =====")
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

                    val hasherFactory = new LoadHasherFactory(Paths.get(hashersPath, s"$name/$desiredSize"))
                    VrlshKnnConstructionAlgorithm.createAndStore(data, hasherFactory, desiredSize, baseDirectory)
                }
            }
        }
    }

    def time(spark: SparkSession,
             testOptions: TestOptions,
             hashersPath: String,
             knnQueryPath: String,
             outputPath: String): Unit = {
        val sc = spark.sparkContext
        val distanceEvaluator = new KnnEuclideanSquareDistance

        println("=====> time =====")
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
                        val hasherFactory = new LoadHasherFactory(Paths.get(hashersPath, s"$name/$desiredSize"))

                        val begin = System.currentTimeMillis()
                        VrlshKnnConstructionAlgorithm.time_createAndStore(data, hasherFactory, desiredSize, baseDirectory)
                        System.currentTimeMillis() - begin
                    }

                    val queryTime = {
                        // Se construye/deserializa el objeto knnQuery
                        val knnQuery = VrlshKnnConstructionAlgorithm.load(sc,
                            Paths.get(knnQueryPath, s"$name/$desiredSize"))

                        val begin2 = System.currentTimeMillis()
                        time_doQueriesWithResult(knnQuery, distanceEvaluator, k, queries.toList, new DefaultStatisticsCollector())
                        (System.currentTimeMillis() - begin2) / size.toDouble
                    }

                    (name, t, k, desiredSize, constructionTime, queryTime)
                }
            }
        }

        val names = testOptions.datasets.reduce((a, b) => a + "_" + b)
        val outputDirectory = Paths.get(outputPath, s"$names")

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

                    val knnQuery = VrlshKnnConstructionAlgorithm.load(spark.sparkContext,
                        Paths.get(outputPath, s"$name/$desiredSize"))
                    knnQuery.printResume()

                    println("==================================================")
                    val numTables = knnQuery.getGeneralStatistics().numTables
                    println(s"Num. tables: $numTables")
                    val points = spark.sparkContext.parallelize(
                        knnQuery.radiuses.flatMap(radius => knnQuery.mapRadiusHashToPoints(radius).toSeq)
                            .flatMap { case (_, ids) => ids })
                    val idAndCount = points
                        .map(id => (id, 1))
                        .reduceByKey { case (c1, c2) => c1 + c2 }

                    idAndCount
                        .map { case (id, count) => (count, 1) }
                        .reduceByKey { case (c1, c2) => c1 + c2 }
                        .collect()
                        .sortWith { case ((c1, _), (c2, _)) => c1.compare(c2) < 0 }
                        .foreach { case (countOfIds, count) => println(s"$countOfIds - $count") }

                }
            }
        }
    }

    def groundTruthAll(spark: SparkSession,
                       testOptions: TestOptions,
                       k: Integer,
                       inputPath: String): Unit = {
        val sc = spark.sparkContext

        println("=====> groundTruthAll =====")
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

        println("=====> maxDistances =====")
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

    def evaluate(spark: SparkSession,
                 testOptions: TestOptions,
                 knnQueryPath: String,
                 outputPath: String): Unit = {
        val sc = spark.sparkContext

        val _statistics = new DefaultStatisticsCollector()

        println("=====> evaluate =====")
        testOptions.datasets.foreach { name =>
            println(s"  Dataset $name")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val queries = testOptions.loadTestFileWithId(sc, name)

            val distanceEvaluator = new KnnEuclideanSquareDistance

            testOptions.ts.foreach { t =>
                testOptions.ks.foreach { k =>
                    val desiredSize = t * k

                    // Se construye/deserializa el objeto knnQuery
                    val knnQuery = VrlshKnnConstructionAlgorithm.load(sc,
                        Paths.get(knnQueryPath, s"$name/$desiredSize"))

                    val statistics = new DefaultStatisticsCollector()

                    print("==== Evaluating:")
                    val count = queries.size
                    val result = doQueriesWithResult(knnQuery, distanceEvaluator, k, queries.toList, statistics)
                        .zipWithIndex
                        .map { case (value, index) => {
                            if (index % 100 == 0) {
                                print(s"  ${100 * index / count}%")
                                Console.flush()
                            }
                            if (index == (count - 1)) {
                                println()
                            }

                            value
                        }
                        }

                    val outputDirectory = Paths.get(outputPath, s"$name/$desiredSize")
                    Files.createDirectories(outputDirectory)

                    statistics.statistics.foreach(v => {
                        v.dataset = name
                        v.t = t
                        v.k = k
                    })

                    _statistics.statistics = _statistics.statistics ++ statistics.statistics

                    CsvUtils.storeApproximateResult(outputDirectory, result, k)
                }
            }
        }

        val names = testOptions.datasets.reduce((a, b) => a + "_" + b)
        val _outputDirectory = Paths.get(outputPath, s"$names")

        _statistics.csv(_outputDirectory.resolve("statistics.csv"))
    }

    def evaluateError(spark: SparkSession,
                      testOptions: TestOptions,
                      groundtruthPath: String,
                      maxDistancesPath: String,
                      knnQueryPath: String,
                      approxPath: String,
                      outputPath: String): Unit = {
        val sc = spark.sparkContext

        val names = testOptions.datasets.reduce((a, b) => a + "_" + b)
        val outputDirectory = Paths.get(outputPath, s"$names")

        println("=====> evaluateError =====")
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

                    val groundTruth = SparkUtils.readGroundTruthFile(sc,
                        Paths.get(groundtruthPath, s"$name/$GROUND_TRUTH_SIZE/groundtruth.csv"))

                    val maxDistances = SparkUtils.readMaxDistancesFile(sc,
                        Paths.get(maxDistancesPath, s"$name/max-distances.csv"))


                    // Se construye/deserializa el objeto knnQuery
                    val knnQuery = VrlshKnnConstructionAlgorithm.load(data.sparkContext,
                        Paths.get(knnQueryPath, s"$name/$desiredSize"))

                    val approximateResult = SparkUtils.readApproxFile(sc,
                        Paths.get(approxPath, s"$name/$desiredSize/result.csv"))

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
                    val knnQuery = VrlshKnnConstructionAlgorithm.load(data.sparkContext,
                        Paths.get(knnQueryPath, s"$name/$desiredSize"))

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

    def evaluateRecallAndMap(spark: SparkSession,
                             testOptions: TestOptions,
                             groundtruthPath: String,
                             maxDistancesPath: String,
                             knnQueryPath: String,
                             approxPath: String): Unit = {
        val sc = spark.sparkContext

        println("=====> evaluateRecallAndMap =====")

        val quality = testOptions.datasets.foreach { name =>

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val queries = testOptions.loadTestFileWithId(sc, name)

            testOptions.ts.foreach { t =>
                testOptions.ks.foreach { k =>
                    val desiredSize = t * k

                    val groundTruth = SparkUtils.readGroundTruthFile(sc,
                        Paths.get(groundtruthPath, s"$name/$GROUND_TRUTH_SIZE/groundtruth.csv"))

                    val maxDistances = SparkUtils.readMaxDistancesFile(sc,
                        Paths.get(maxDistancesPath, s"$name/max-distances.csv"))

                    // Se construye/deserializa el objeto knnQuery
                    val knnQuery = VrlshKnnConstructionAlgorithm.load(data.sparkContext,
                        Paths.get(knnQueryPath, s"$name/$desiredSize"))

                    val approximateResult = SparkUtils.readApproxFile(sc,
                        Paths.get(approxPath, s"$name/$desiredSize/result.csv"))

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

                    val result = statistics.map(s => (s.recall, s.avgPrecision, 1))
                        .reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

                    val recall = result._1 / result._3.toDouble
                    val map = result._2 / result._3.toDouble
                    println(f"    name: $name t: $t k: $k recall: $recall%1.5f MAP: $map%1.5f")
                }
            }
        }
    }
}
