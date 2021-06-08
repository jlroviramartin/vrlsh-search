package org.example.testing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import org.example.Utils
import org.example.SparkUtils

import java.nio.file.{Path, Paths}
import scala.collection.mutable

class TestOptions(var datasets: Array[String] = Array("corel", "shape", "audio"),
                  var ts: Array[Int] = Array(5, 10, 20),
                  var ks: Array[Int] = Array(4, 16, 20, 64, 256),
                  var datasetPath: String = "C:/Users/joseluis/OneDrive/TFM/dataset",
                  var trainRatio: Double = -1,
                  var samples: Int = -1) {

    var dataFilePath: Map[String, Path] = datasets.map(name => (name, Paths.get(datasetPath, s"$name/${name}_i1_90.csv"))).toMap
    var testFilePath: Map[String, Path] = datasets.map(name => (name, Paths.get(datasetPath, s"$name/${name}_i1_10.csv"))).toMap

    def getDataFilePath(name: String): Path = {
        dataFilePath(name)
    }

    private def getTestFilePath(name: String): Path = {
        testFilePath(name)
    }

    def loadDataFile(sc: SparkContext, name: String): RDD[(Long, Vector)] = {
        val rdd = SparkUtils.readDataFileByFilename(sc, getDataFilePath(name).toString)

        if (trainRatio > 0) {
            rdd.randomSplit(Array(trainRatio, 1 - trainRatio), seed = Utils.RANDOM_SEED)(0)
        } else {
            rdd
        }
    }

    def loadTestFile(sc: SparkContext, name: String): Array[Vector] = {
        if (trainRatio > 0) {
            SparkUtils.readDataFileByFilename(sc, getDataFilePath(name).toString)
                .map { case (id, point) => point }
                .randomSplit(Array(trainRatio, 1 - trainRatio), seed = Utils.RANDOM_SEED)(1)
                .collect()
        } else {
            val rdd = SparkUtils.readDataFileByFilename(sc, getTestFilePath(name).toString)
                .map { case (id, point) => point }

            if (samples > 0) {
                rdd.takeSample(withReplacement = false, samples, Utils.RANDOM_SEED)
            } else {
                rdd.collect()
            }
        }
    }

    def loadTestFileWithId(sc: SparkContext, name: String): Array[(Long, Vector)] = {
        if (trainRatio > 0) {
            SparkUtils.readDataFileByFilename(sc, getDataFilePath(name).toString)
                .randomSplit(Array(trainRatio, 1 - trainRatio), seed = Utils.RANDOM_SEED)(1)
                .collect()
        } else {
            val rdd = SparkUtils.readDataFileByFilename(sc, getTestFilePath(name).toString)

            if (samples > 0) {
                rdd.takeSample(withReplacement = false, samples, Utils.RANDOM_SEED)
            } else {
                rdd.collect()
            }
        }
    }
}
