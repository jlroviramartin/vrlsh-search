package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel}

import java.nio.file.{Files, Paths}
import java.util.Date
import org.example.testing.{DefaultErrorCollector, TestOptions}

object LshSparkApp {

    def main(args: Array[String]): Unit = {
        val spark = SparkUtils.initSpark()

        val testOptions = new TestOptions()
        testOptions.ks = Array(20)

        //storeAll(spark, testOptions)
        testAll(spark, testOptions)

        //writeAllHashers(spark)
    }

    private def testAll(spark: SparkSession,
                        testOptions: TestOptions,
                        inputPath: String = "C:/result/lshspark"): Unit = {
        val sc = spark.sparkContext
        import spark.implicits._

        val distanceEvaluator = new KnnEuclideanDistance

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)
            val count = data.count()

            // Testing data
            val testing = testOptions.loadTestFile(sc, name)

            testOptions.ts.foreach { t =>
                println(s"  === $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                    val errorCollector = new DefaultErrorCollector(count, k)

                    val baseDirectory = Paths.get(inputPath, s"$name/$t/$k")
                    Files.createDirectories(baseDirectory)

                    val baseFile = baseDirectory.resolve("model.dat")

                    val df = data.toDF("id", "features")
                    //val df = spark.createDataFrame(data).toDF("id", "features")

                    //val model = BucketedRandomProjectionLSHModel.read.load(baseFile.toString)
                    val model = {
                        val dimension = data.first()._2.size
                        val numTables = Math.floor(Math.pow(Utils.log2(dimension), 2)).toInt

                        val bucketLength = Math.pow(count, -1.0 / dimension)
                        //val x2 = Math.pow(54387, -1.0 / 192)
                        //val x3 = Math.pow(28775, -1.0 / 544)

                        val brp = new BucketedRandomProjectionLSH()
                            .setBucketLength(0.01) // radius
                            .setNumHashTables(2)
                            .setInputCol("features")
                            .setOutputCol("hashes")
                            .setSeed(Utils.RANDOM_SEED)

                        val df = spark.createDataFrame(data).toDF("id", "features")

                        brp.fit(df)
                    }

                    var i = 0
                    testing.foreach(query => {
                        println(s"      $i =====")
                        i = i + 1

                        val approximateResultDs = model.approxNearestNeighbors(
                            df,
                            query,
                            20) //k)
                        //approximateResultDs.show()

                        val h = approximateResultDs
                            .toDF()
                            .map(row => row(2).toString)
                            .collect()
                            .toIndexedSeq

                        val approximateResult = approximateResultDs
                            .toDF()
                            .map(row => (row(3).asInstanceOf[Double], row(0).asInstanceOf[Long]))
                            .collect()
                            .sortBy { case (distance, id) => distance }
                            .toIndexedSeq

                        Errors.checkError(
                            data,
                            approximateResult, // Iterable[(distance: Double, id: Long)]
                            distanceEvaluator,
                            20, //k,
                            query,
                            errorCollector)
                    })

                    errorCollector.csv(baseDirectory.resolve("error.csv"))
                    errorCollector.showAverageOfErrors()
                }
            }
        }
    }

    private def storeAll(spark: SparkSession,
                         testOptions: TestOptions,
                         outputPath: String = "C:/result/lshspark"): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)
            val dimension = data.first()._2.size

            testOptions.ts.foreach { t =>
                println(s"===== $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"===== $k: ${new Date(System.currentTimeMillis())} =====")

                    val baseDirectory = Paths.get(outputPath, s"$name/$t/$k")
                    Files.createDirectories(baseDirectory)

                    val desiredSize = t * k
                    val baseFile = baseDirectory.resolve("model.dat")

                    storeModel(spark, baseFile.toString, data, dimension, desiredSize)
                }
            }
        }
    }

    private def storeModel(spark: SparkSession,
                           modelPath: String,
                           data: RDD[(Long, Vector)],
                           dimension: Int,
                           desiredSize: Int) = {
        val numTables = Math.floor(Math.pow(Utils.log2(dimension), 2)).toInt

        val brp = new BucketedRandomProjectionLSH()
            .setBucketLength(desiredSize)
            .setNumHashTables(numTables)
            .setInputCol("features")
            .setOutputCol("hashes")
            .setSeed(Utils.RANDOM_SEED)

        val df = spark.createDataFrame(data).toDF("id", "features")

        val model = brp.fit(df)
        model.write.overwrite().save(modelPath)
    }
}
