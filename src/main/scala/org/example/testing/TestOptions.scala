package org.example.testing

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.storage.StorageLevel
import org.example.Utils
import org.example.SparkUtils

import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable

class TestOptions(var datasets: Array[String] = Array("corel", "shape", "audio"),
                  var ts: Array[Int] = Array(5, 10, 20),
                  var ks: Array[Int] = Array(4, 16, 20, 64, 256),
                  var datasetPath: String = "C:/Users/joseluis/OneDrive/TFM/dataset",
                  var trainRatio: Double = -1,
                  var samples: Int = -1) {

    var dataFilePath: Map[String, Path] = datasets.map(name => (name, Paths.get(datasetPath, s"$name/${name}_i1_90.csv"))).toMap
    var testFilePath: Map[String, Path] = datasets.map(name => (name, Paths.get(datasetPath, s"$name/${name}_i1_10.csv"))).toMap

    val cacheData: mutable.Map[String, RDD[(Long, Vector)]] = mutable.Map()
    val cacheTest1: mutable.Map[String, Array[Vector]] = mutable.Map()
    val cacheTest2: mutable.Map[String, Array[(Long, Vector)]] = mutable.Map()

    def getDataFilePath(name: String): Path = {
        dataFilePath(name)
    }

    private def getTestFilePath(name: String): Path = {
        testFilePath(name)
    }

    def loadDataFile(sc: SparkContext, name: String): RDD[(Long, Vector)] = {
        cacheData.getOrElseUpdate(name, {
            var fileName = getDataFilePath(name).toString

            if (trainRatio > 0) {
                val file = Paths.get(getDataFilePath(name) + f"${trainRatio % 1.5f}-data.csv")
                FileUtils.deleteDirectory(file.toFile)
                fileName = file.toString

                sc.textFile(getDataFilePath(name).toString)
                    .randomSplit(Array(trainRatio, 1 - trainRatio), seed = Utils.RANDOM_SEED)(0)
                    .saveAsTextFile(fileName)
            }

            SparkUtils.readDataFileByFilename(sc, fileName)
        })
    }

    def loadTestFile(sc: SparkContext, name: String): Array[Vector] = {
        cacheTest1.getOrElseUpdate(name, {
            var fileName = getTestFilePath(name).toString

            if (trainRatio > 0) {
                val file = Paths.get(getDataFilePath(name) + f"${trainRatio % 1.5f}-test.csv")
                FileUtils.deleteDirectory(file.toFile)
                fileName = file.toString

                sc.textFile(getDataFilePath(name).toString)
                    .randomSplit(Array(trainRatio, 1 - trainRatio), seed = Utils.RANDOM_SEED)(1)
                    .saveAsTextFile(fileName)
            }

            val rdd = SparkUtils.readDataFileByFilename(sc, fileName)
                .map { case (id, point) => point }

            if (samples > 0) {
                rdd.takeSample(withReplacement = false, samples, Utils.RANDOM_SEED)
            } else {
                rdd.collect()
            }
        })
    }

    def loadTestFileWithId(sc: SparkContext, name: String): Array[(Long, Vector)] = {
        cacheTest2.getOrElseUpdate(name, {
            var fileName = getTestFilePath(name).toString

            if (trainRatio > 0) {
                val file = Paths.get(getDataFilePath(name) + f"${trainRatio % 1.5f}-test.csv")
                FileUtils.deleteDirectory(file.toFile)
                fileName = file.toString

                sc.textFile(getDataFilePath(name).toString)
                    .randomSplit(Array(trainRatio, 1 - trainRatio), seed = Utils.RANDOM_SEED)(1)
                    .saveAsTextFile(fileName)
            }

            val rdd = SparkUtils.readDataFileByFilename(sc, fileName)

            if (samples > 0) {
                rdd.takeSample(withReplacement = false, samples, Utils.RANDOM_SEED)
            } else {
                rdd.collect()
            }
        })
    }
}
