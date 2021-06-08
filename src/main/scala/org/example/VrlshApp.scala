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
            testOptions.datasets = Array(/*"siftsmall", "audio",*/ "sift")
            testOptions.trainRatio = 0.999 // 1000 testing
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

            val DIRECTORY = "C:/result/vrlsh-5"

            // Se guardan los hashers: DIRECTORY/<name>/<desiredSize>/hasher.dat
            storeAllHashers(spark, testOptions, DIRECTORY)

            // Se guardan los resultados:
            storeAll(spark, testOptions, DIRECTORY)

            // Se guardan las máximas distancias: DIRECTORY/<name>/max-distances.csv
            maxDistances(spark, testOptions, DIRECTORY)

            // Se guardan los resultados reales: DIRECTORY/groundtruth/<name>/<size>/groundtruth.csv
            groundTruthAll(spark, testOptions, s"$DIRECTORY/groundtruth", 1000)

            // Se evaluan los knn: DIRECTORY/result/<name>/<k>/result.csv
            //                     DIRECTORY/result/<name>/<k>/statistics.csv
            evaluations(spark, testOptions, DIRECTORY)

            // Se evalua el error: DIRECTORY/statistics/<name>/global-statistics.csv
            //                     DIRECTORY/statistics/<name>/quality-statistics.csv
            evaluateError(spark, testOptions, DIRECTORY)

            // Se calculan los valores - No necesario
            // ------> testAll(spark, testOptions, DIRECTORY)
        }

        // Calcula las distancias máximas
        {
            //val testOptions = new TestOptions()
            //testOptions.useSamples = false
            //maxDistances(spark, testOptions, "C:/result/max-distances2")
        }

        // Se evalua
        {
            val testOptions = new TestOptions()
            testOptions.samples = 1000
            testOptions.ts = Array(5, 10, 20, 40, 80)
            testOptions.ks = Array(100)
            //storeAll(spark, testOptions, "C:/result/vrlsh-2")
        }


        // Calcula los valores reales
        /*{
            val testOptions = new TestOptions()
            testOptions.useSamples = false
            testOptions.ks = Array(1000)
            //groundTruthAll(spark, testOptions, "C:/result/groundtruth")
        }*/

        // Se parten los ficheros en training + testing
        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/corel/corel_i1.csv", 90)
        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/shape/shape_i1.csv", 90)
        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/audio/audio_i1.csv", 90)

        // Se evalua
        {
            /*val testOptions = new TestOptions()
            testOptions.samples = 1000
            testOptions.ts = Array(5, 10, 20, 40, 80)
            testOptions.ks = Array(20)
            showInfo(spark, testOptions, "C:/result/vrlsh")*/
        }

        /*println(s"Start: ${new Date(System.currentTimeMillis())}")

        {
            val testOptions = new TestOptions()
            testOptions.samples = 1000
            testOptions.ts = Array(5, 10, 20, 40, 80)
            testOptions.ks = Array(20)
            testAll(spark, testOptions, "C:/result/vrlsh")
        }

        println(s"Finish: ${new Date(System.currentTimeMillis())}")*/

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

    def testAll(spark: SparkSession,
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

    /*def showInfo(spark: SparkSession,
                 testOptions: TestOptions,
                 inputPath: String): Unit = {
        val sc = spark.sparkContext

        var i = 1
        testOptions.datasets.foreach { name =>
            println(s"## $i. Dataset $name")
            println()

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

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

            testOptions.ts.foreach { t =>
                println(s"### t=$t")
                println()

                testOptions.ks.foreach { k =>
                    println(s"#### K=$k")
                    println()

                    val desiredSize = t * k

                    println(f"Desired size: $desiredSize [${MIN_TOLERANCE * desiredSize}, ${MAX_TOLERANCE * desiredSize}]")
                    println()

                    // Se construye/deserializa el objeto knnQuery
                    val baseDirectory = Paths.get(inputPath, s"$name/$t/$k")
                    val knnQuery = VrlshKnnConstructionAlgorithm.load(data.sparkContext, baseDirectory).asInstanceOf[VrlshKnnQuery]

                    val generalStatistics = knnQuery.getGeneralStatistics()
                    generalStatistics.dataset = name
                    generalStatistics.t = t
                    generalStatistics.k = k

                    println("Evaluación")
                    println("---")
                    println()

                    println(s"Index error avg.: `r mean((errors %>% filter(t == $t & k == $k))$$avgIndexError) `")
                    println()

                    println(s"Distance error avg.: `r mean((errors %>% filter(t == $t & k == $k))$$avgDistanceError) `")
                    println()

                    println(s"Recall avg.: `r mean((errors %>% filter(t == $t & k == $k))$$recall) `")
                    println()

                    println(s"Size avg.: `r mean((errors %>% filter(t == $t & k == $k))$$size) `")
                    println()

                    //println(s"Size avg.: `r mean((statistics %>% filter(t == $t & k == $k))$$size) `")
                    //println()

                    println(s"Comparisons avg.: `r mean((statistics %>% filter(t == $t & k == $k))$$comparisons) `")
                    println()

                    println(s"Buckets avg.: `r mean((statistics %>% filter(t == $t & k == $k))$$buckets) `")
                    println()

                    println(s"Number of levels avg.: `r mean((statistics %>% filter(t == $t & k == $k))$$numLevels) `")
                    println()
                }
            }

            println("### Gráficas de error:")
            println()

            println(s"```{r Error $name, echo=FALSE, fig.height=10, fig.width=10}")
            println("showErrors(\"" + name + "\")")
            println("```")
            println()

            println("### Gráficas de rendimiento:")
            println()

            println(s"```{r Statistics $name, echo=FALSE, fig.height=10, fig.width=10}")
            println("showStatistics(\"" + name + "\")")
            println("```")
            println()

            i = i + 1
        }
    }*/

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
