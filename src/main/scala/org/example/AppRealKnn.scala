package org.example

import org.example.SimpleApp.{initSpark, readDataFileByFilename}
import org.example.testing.KnnTest
import org.example.testing.TestingUtils.doRealQueries_v2

import java.nio.file.{Files, Paths}
import java.util.Date

object AppRealKnn {

    def main(args: Array[String]) {
        val spark = initSpark()
        val sc = spark.sparkContext

        val distanceEvaluator = new KnnEuclideanSquareDistance

        List("corel"/*, "shape", "audio"*/).foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = readDataFileByFilename(sc, s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_90.csv")

            // Testing data
            val testing = readDataFileByFilename(sc, s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_10.csv")
            //    .takeSample(withReplacement = false, 10, Utils.RANDOM.nextLong())

            val k = 10000
            println(s"===== $k: ${new Date(System.currentTimeMillis())} =====")

            val outputDirectory = Paths.get(s"C:/Users/joseluis/OneDrive/TFM/result")
            Files.createDirectories(outputDirectory)

            var i = 0
            val part = 1000
            testing.collect().sliding(part, part)
                .foreach(slide => {
                    println(s"== ${i * part}")
                    if (i > 36) {

                        val result = doRealQueries_v2(data, distanceEvaluator, k, slide)
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
