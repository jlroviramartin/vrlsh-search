package org.example

import org.example.testing.{TestOptions, TestingUtils}

import java.nio.file.{Files, Paths}
import java.util.Date

object AppRealKnn {

    def main(args: Array[String]) {
        val spark = SparkUtils.initSpark()
        val sc = spark.sparkContext

        val testOptions = new TestOptions()
        testOptions.ks = Array(20)

        val distanceEvaluator = new KnnEuclideanSquareDistance

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = testOptions.loadDataFile(sc, name)

            // Testing data
            val testing = testOptions.loadTestFileWithId(sc, name)

            val k = 10000
            println(s"===== $k: ${new Date(System.currentTimeMillis())} =====")

            val outputDirectory = Paths.get(s"C:/Users/joseluis/OneDrive/TFM/result")
            Files.createDirectories(outputDirectory)

            var i = 0
            val part = 1000
            testing.sliding(part, part)
                .foreach(slide => {
                    println(s"== ${i * part}")
                    if (i > 44) {

                        val result = TestingUtils.doGroundTruth(data, distanceEvaluator, k, slide)
                            .toMap

                        println()

                        DataStore.kstore(outputDirectory.resolve(s"knnreal_${name}_${k}_$i"), result)
                    }
                    i = i + 1
                })

            //val result2 = DataStore.kload_v2[Map[Long, (Array[(Long, Double)], (Long, Double))]](outputDirectory.resolve(s"knnreal_${name}_${k}"))
        }

        println(s"Finish: ${new Date(System.currentTimeMillis())}")

        spark.stop()
    }
}
