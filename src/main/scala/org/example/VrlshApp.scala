package org.example

import org.apache.spark.sql.SparkSession
import org.example.construction.{KnnConstructionAlgorithm, MyKnnQuery}
import org.example.testing.{KnnTest, TestOptions}

import java.nio.file.{Files, Paths}
import java.util.{Calendar, Date}
import org.example.Utils.{MAX_TOLERANCE, MIN_TOLERANCE, RANDOM_SEED, time}
import org.example.evaluators.{Hasher, HasherFactory, LoadHasherFactory}

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import scala.collection._
import org.example.SparkUtils

// How to turn off INFO logging in Spark? https://stackoverflow.com/a/26123496
object VrlshApp {
    def main(args: Array[String]) {

        val fos = new FileOutputStream(new File("C:/Temp/log.txt"))

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
            testOptions.useSamples = false
            testOptions.datasets = Array(/*"audio", */ "sift" /* , "gist"*/)
            testOptions.dataFilePath = immutable.Map(
                "audio" -> Paths.get("C:\\datasets\\audio\\audio_base.csv"),
                "gist" -> Paths.get("C:\\datasets\\gist\\gist_base.csv"),
                "sift" -> Paths.get("C:\\datasets\\sift\\sift_base.csv"))
            testOptions.testFilePath = immutable.Map(
                "audio" -> Paths.get("C:\\datasets\\audio\\audio_query.csv"),
                "gist" -> Paths.get("C:\\datasets\\gist\\gist_query.csv"),
                "sift" -> Paths.get("C:\\datasets\\sift\\sift_query.csv"))
            testOptions.ts = Array(50)
            testOptions.ks = Array(4, 16, 20, 64, 256)

            //storeAllHashers(spark, testOptions, "C:/result/vrlsh-3")

            storeAll(spark, testOptions, "C:/result/vrlsh-3")
            //maxDistances(spark, testOptions, "C:/result/max-distances2")
        }

        // Calcula las distancias máximas
        {
            val testOptions = new TestOptions()
            testOptions.useSamples = false
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

        //spark.stop()
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

                    val baseDirectory = Paths.get(outputPath, s"$name")
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

                    val baseDirectory = Paths.get(inputPath, s"$name/$t/$k")
                    Files.createDirectories(baseDirectory)

                    KnnTest.testSet_v2(data, baseDirectory, baseDirectory, testing.to[immutable.Iterable], k, t)
                }
            }
        }
    }

    def groundTruthAll(spark: SparkSession,
                       testOptions: TestOptions,
                       inputPath: String): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val testing = testOptions.loadTestFileWithId(sc, name)

            testOptions.ks.foreach { k =>
                println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                val baseDirectory = Paths.get(inputPath, s"$name/$k")
                Files.createDirectories(baseDirectory)

                KnnTest.storeGroundTruth(data, baseDirectory, testing.to[immutable.Iterable], k)
            }
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
            val testing = testOptions.loadTestFileWithId(sc, name)

            println(s"    ${new Date(System.currentTimeMillis())} =====")

            val baseDirectory = Paths.get(inputPath, s"$name")
            Files.createDirectories(baseDirectory)

            KnnTest.storeMaxDistances(data, baseDirectory, testing.to[immutable.Iterable])
        }
    }

    def showInfo(spark: SparkSession,
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
                    val knnQuery = KnnConstructionAlgorithm.load(data.sparkContext, baseDirectory).asInstanceOf[MyKnnQuery]
                    knnQuery.printResume()

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
    }
}
