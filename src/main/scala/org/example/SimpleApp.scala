package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.example.testing.KnnTest

import java.nio.file.{Files, Path, Paths}

// How to turn off INFO logging in Spark? https://stackoverflow.com/a/26123496
object SimpleApp {
    def main(args: Array[String]) {
        val spark = initSpark()
        val sc = spark.sparkContext

        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/corel/corel_i1.csv", 90)
        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/shape/shape_i1.csv", 90)
        //Utils.splitDataByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/audio/audio_i1.csv", 90)

        //val fileToRead = "hdfs://namenode:9000/test.txt"
        //val fileToRead = Paths.get("C:/Users/joseluis/OneDrive/TFM/dataset/HIGGS_head_numbered_100000.csv")

        //val fileToRead = Paths.get("C:/Users/joseluis/OneDrive/TFM/dataset/corel/corel_i1.csv")
        //val fileToRead = Paths.get("C:/Users/joseluis/OneDrive/TFM/dataset/shape/shape_i1.csv")
        //val fileToRead = Paths.get("C:/Users/joseluis/OneDrive/TFM/dataset/audio/audio_i1.csv")

        //val testing = readDataFileByFilename(sc, "C:/Users/joseluis/OneDrive/TFM/dataset/corel/corel_i1_10.csv")

        Files.createDirectories(Paths.get("C:/Temp/knn/"))

        List("corel", "shape", "audio").foreach { name =>
            println(s"===== $name =====")
            val data = readDataFileByFilename(sc, s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_90.csv")
            val testing = readDataFileByFilename(sc, s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_10.csv")
                .map { case (id, point) => point }
                .takeSample(withReplacement = false, 10)

            List(3, 10, 50, 200).foreach { k =>
                println(s"===== $k =====")

                val baseDirectory = Paths.get(s"C:/Temp/$name/$k")
                if (false) {
                    KnnTest.prepareData_v2(data, baseDirectory, k)
                } else {
                    KnnTest.testSet_v2(data, baseDirectory, testing, k)
                }
            }
        }

        /*val k = 10
        val baseDirectory = Paths.get(s"C:/Temp/knn/knn_$k/")
        KnnTest.testSet_v2(data,
            baseDirectory,
            testing.map(row => row._2).collect(),
            k)*/

        //KnnTest.testSet_v2(envelope, data.cache(), baseDirectory, desiredSize)
        //KnnTest.testSet2(data, baseDirectory, desiredSize, 10, 10)
        spark.stop()
    }

    def initSpark(): SparkSession = {
        // https://dzone.com/articles/working-on-apache-spark-on-windows
        // https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
        // https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3
        // SPARK_HOME
        // HADOOP_HOME
        System.setProperty("hadoop.home.dir", "C:\\Users\\joseluis\\OneDrive\\Aplicaciones\\spark-3.0.1-hadoop-3.2")
        //System.setProperty("hadoop.home.dir", "C:\\Users\\joseluis\\OneDrive\\Aplicaciones\\spark-2.4.7-hadoop-2.7")

        // http://spark.apache.org/docs/latest/monitoring.html
        System.setProperty("spark.ui.port", "44041")

        Utils.quiet_logs()

        val spark = SparkSession.builder
            .appName("Simple Application")
            .config("spark.master", "local")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer", "64m")
            .config("spark.kryoserializer.buffer.max", "128m")
            .getOrCreate()

        spark
    }

    def readDataFileByFilename(sc: SparkContext, file: String): RDD[(Long, Vector)] = {
        readDataFile(sc, Paths.get(file))
    }

    def readDataFile(sc: SparkContext, file: Path): RDD[(Long, Vector)] = {
        val data: RDD[(Long, Vector)] = if (true) {
            sc.textFile(file.toString)
                .map(line => {
                    val args = line.split(',')
                    val id = args(0).toLong
                    val values = (1 until args.length).map(i => args(i).toDouble).toArray
                    (id, Vectors.dense(values))
                })
        } else {
            // No funciona ???
            MLUtils.loadLibSVMFile(sc, file.toString)
                .zipWithIndex()
                .map(_.swap)
                .map { case (id, labelPoint) => (id, Vectors.dense(labelPoint.features.toArray)) }
        }
        data
    }
}
