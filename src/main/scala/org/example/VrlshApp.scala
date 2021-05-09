package org.example

import org.apache.spark.sql.SparkSession
import org.example.construction.{KnnConstructionAlgorithm, MyKnnQuery}
import org.example.testing.{KnnTest, TestOptions}

import java.nio.file.{Files, Paths}
import java.util.Date
import org.example.Utils.{MAX_TOLERANCE, MIN_TOLERANCE, RANDOM_SEED}

// How to turn off INFO logging in Spark? https://stackoverflow.com/a/26123496
object VrlshApp {
    def main(args: Array[String]) {
        val spark = SparkUtils.initSpark()

        {
            val testOptions = new TestOptions()
            testOptions.useSamples = false
            testOptions.ks = Array(1000)
            groundTruthAll(spark, testOptions, "C:/result/groundtruth")
        }

        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/corel/corel_i1.csv", 90)
        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/shape/shape_i1.csv", 90)
        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/audio/audio_i1.csv", 90)

        /*val testOptions = new TestOptions()
        testOptions.samples = 1000
        testOptions.ts = Array(5, 10, 20, 40, 80)
        testOptions.ks = Array(20)
        showInfo(spark, testOptions, "C:/result/vrlsh")*/

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

                    val baseDirectory = Paths.get(outputPath, s"$name/$t/$k")
                    Files.createDirectories(baseDirectory)

                    KnnTest.createAndStore(data, baseDirectory, k, t)
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

                    KnnTest.testSet_v2(data, baseDirectory, baseDirectory, testing, k, t)
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

                KnnTest.storeGroundTruth(data, baseDirectory, testing.toIterable, k)
            }
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
