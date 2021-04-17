package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import org.example.testing.KnnTest

import java.nio.file.{Files, Path, Paths}
import java.util.Date
import scala.reflect.io.Directory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

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

        println(s"Start: ${new Date(System.currentTimeMillis())}")

        List("corel", "shape", "audio").foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = readDataFileByFilename(sc, s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_90.csv")

            // Testing data
            val testing = readDataFileByFilename(sc, s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_10.csv")
                .map { case (id, point) => point }
                .takeSample(withReplacement = false, 100, Utils.RANDOM.nextLong())

            //List(3, 10, 50, 200).foreach { k =>
            List(5, 10, 20).foreach { t =>
                println(s"===== $t: ${new Date(System.currentTimeMillis())} =====")

                List(4, 16, 64, 256).foreach { k =>
                    println(s"===== $k: ${new Date(System.currentTimeMillis())} =====")

                    val outputDirectory = Paths.get(s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/$t/$k")
                    Files.createDirectories(outputDirectory)

                    val baseDirectory = Paths.get(s"C:/Temp/$name/$t/$k")
                    if (false) {
                        KnnTest.prepareData_v2(data, baseDirectory, k, t)
                    } else {
                        KnnTest.testSet_v2(data, baseDirectory, outputDirectory, testing, k, t)
                    }
                }
            }
        }

        println(s"Finish: ${new Date(System.currentTimeMillis())}")

        spark.stop()
    }

    def copyToHdfs(): Unit = {
        val hadoopConf = new Configuration()
        val hdfs = FileSystem.get(hadoopConf)

        List("corel", "shape", "audio").foreach { name =>
            println(s"===== Copiando $name =====")

            val srcDataset = new org.apache.hadoop.fs.Path(s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_90.csv")
            val destDataset = new org.apache.hadoop.fs.Path(s"/dataset/$name/${name}_i1_90.csv")
            hdfs.copyFromLocalFile(srcDataset, destDataset)

            val srcTesting = new org.apache.hadoop.fs.Path(s"C:/Users/joseluis/OneDrive/TFM/dataset/$name/${name}_i1_10.csv")
            val destTesting = new org.apache.hadoop.fs.Path(s"/dataset/$name/${name}_i1_10.csv")
            hdfs.copyFromLocalFile(srcTesting, destTesting)
        }
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
            .config("spark.driver.cores", "10")
            .config("spark.driver.memory", "16g")
            .config("spark.executor.cores", "10")
            .config("spark.executor.memory", "16g")
            .config("spark.master", "local")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer", "64m")
            .config("spark.kryoserializer.buffer.max", "1024m") // 128m
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
