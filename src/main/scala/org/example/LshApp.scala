package org.example

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.construction.DefaultStatisticsCollector
import org.example.lsh.LshKnnConstructionAlgorithm
import org.example.testing.{DefaultErrorCollector, TestOptions}

import java.nio.file.{Files, Paths}
import java.util.Date

object LshApp {

    def main(args: Array[String]): Unit = {
        val spark = SparkUtils.initSpark()

        val testOptions = new TestOptions()
        testOptions.ks = Array(20)

        //storeAll(spark, testOptions)
        testAll(spark, testOptions)

        //writeAllHashers(spark)
    }

    private def testAll(spark: SparkSession,
                        testOptions: TestOptions): Unit = {
        val sc = spark.sparkContext
        import spark.implicits._

        val distanceEvaluator = new KnnEuclideanDistance

        val hashersMap = SparkUtils.readAllHashers(testOptions)

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val originalData = testOptions.loadDataFile(sc, name)
            val count = originalData.count()
            val dimension = originalData.first()._2.size

            val envelope = originalData
                .map { case (id, point) => point }
                .aggregate(EnvelopeDouble.EMPTY)(
                    EnvelopeDouble.seqOp,
                    EnvelopeDouble.combOp
                )
            val benvelope = sc.broadcast(envelope)

            val data = originalData
                .map { case (id, point) => (id, EnvelopeDouble.normalize(benvelope.value, point)) }

            // Testing data
            val originalTesting = testOptions.loadTestFile(sc, name)

            val testing = originalTesting
                .map { case point => EnvelopeDouble.normalize(benvelope.value, point) }

            testOptions.ts.foreach { t =>
                println(s"  === $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"    = $k: ${new Date(System.currentTimeMillis())} =====")

                    val (hasher, hasherOptions, radius) = hashersMap(name, t, k)
                    val numTables = hasherOptions.numTables
                    val keyLength = hasherOptions.keyLength

                    val statistics = new DefaultStatisticsCollector()
                    val errorCollector = new DefaultErrorCollector(count, k)

                    val baseDirectory = Paths.get(s"C:/result/lsh/$name/$t/$k")
                    Files.createDirectories(baseDirectory)

                    val model = LshKnnConstructionAlgorithm.buildForNormalizeData(
                        dimension,
                        numTables,
                        count)
                        .build(data)
                    /*val model = LshKnnConstructionAlgorithm.build(
                        dimension,
                        radius,
                        numTables,
                        keyLength)
                        .build(data)*/

                    var i = 0
                    testing.foreach(query => {
                        println(s"      $i =====")
                        i = i + 1

                        val approximateResult = model.query(
                            query,
                            k,
                            distanceEvaluator,
                            statistics)

                        Errors.checkError(
                            data,
                            approximateResult, // Iterable[(distance: Double, id: Long)]
                            distanceEvaluator,
                            k,
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
                         testOptions: TestOptions): Unit = {
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

                    val baseDirectory = Paths.get(s"C:/result/lshspark/$name/$t/$k")
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
