package org.example.testing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import org.example.Utils
import org.example.SparkUtils

import java.nio.file.{Path, Paths}

class TestOptions(var datasets: Array[String] = Array("corel", "shape", "audio"),
                  var ts: Array[Int] = Array(5, 10, 20),
                  var ks: Array[Int] = Array(4, 16, 20, 64, 256),
                  var datasetPath: String = "C:/Users/joseluis/OneDrive/TFM/dataset",
                  var useSamples: Boolean = true,
                  var samples: Int = 100) {

    def getDataFilePath(name: String): Path = {
        Paths.get(datasetPath, s"$name/${name}_i1_90.csv")
    }

    def getTestFilePath(name: String): Path = {
        Paths.get(datasetPath, s"$name/${name}_i1_10.csv")
    }

    def loadDataFile(sc: SparkContext, name: String): RDD[(Long, Vector)] = {
        //val data = "hdfs://namenode:9000/dataset/$name/${name}_i1_90.csv"
        SparkUtils.readDataFileByFilename(sc, getDataFilePath(name).toString)
    }

    def loadTestFile(sc: SparkContext, name: String): Array[Vector] = {
        if (useSamples) {
            SparkUtils.readDataFileByFilename(sc, getTestFilePath(name).toString)
                .map { case (id, point) => point }
                .takeSample(withReplacement = false, samples, Utils.RANDOM_SEED)
        } else {
            SparkUtils.readDataFileByFilename(sc, getTestFilePath(name).toString)
                .map { case (id, point) => point }
                .collect()
        }
    }

    def loadTestFileWithId(sc: SparkContext, name: String): Array[(Long, Vector)] = {
        if (useSamples) {
            SparkUtils.readDataFileByFilename(sc, getTestFilePath(name).toString)
                .takeSample(withReplacement = false, samples, Utils.RANDOM_SEED)
        } else {
            SparkUtils.readDataFileByFilename(sc, getTestFilePath(name).toString)
                .collect()
        }
    }
}
