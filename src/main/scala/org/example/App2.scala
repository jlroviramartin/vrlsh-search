package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.col
import org.example.SimpleApp.{initSpark, readDataFileByFilename}
import org.example.testing.KnnTest

import java.nio.file.{Files, Paths}
import java.util.Date

object App2 {

/*
    def main(args: Array[String]): Unit = {
        val spark = initSpark()

        List("corel", "shape", "audio").foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = spark.read.csv(s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_90.csv")
                .map()
            data.printSchema()

            // Testing data
            //val testing = spark.read.csv(s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_10.csv")
            //    .map { case (id, point) => point }
            //    .takeSample(withReplacement = false, 100, Utils.RANDOM.nextLong())

            List(4, 16, 64, 256).foreach { k =>
                println(s"===== $k: ${new Date(System.currentTimeMillis())} =====")

            }
        }




        val dfA = spark.createDataFrame(Seq(
            (0, Vectors.dense(1.0, 1.0)),
            (1, Vectors.dense(1.0, -1.0)),
            (2, Vectors.dense(-1.0, -1.0)),
            (3, Vectors.dense(-1.0, 1.0))
        )).toDF("id", "features")

        val key = Vectors.dense(1.0, 0.0)

        val brp = new BucketedRandomProjectionLSH()
            .setBucketLength(2.0)
            .setNumHashTables(3)
            .setInputCol("features")
            .setOutputCol("hashes")

        val model = brp.fit(dfA)

        // Compute the locality sensitive hashes for the input rows, then perform approximate nearest
        // neighbor search.
        // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
        // `model.approxNearestNeighbors(transformedA, key, 2)`
        println("Approximately searching dfA for 2 nearest neighbors of the key:")
        model.approxNearestNeighbors(dfA, key, 2).show()
    }
*/
}
