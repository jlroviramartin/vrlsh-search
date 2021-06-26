package org.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.evaluators.Hasher
import org.example.testing.TestOptions

import java.nio.file.{Files, Path, Paths}
import java.util.Date
import scala.collection.mutable.Map

object SparkUtils {

    // Utility functions

    def writeAllHashers(spark: SparkSession,
                        testOptions: TestOptions,
                        inputPath: String = "C:/result/hasher"): Unit = {
        val sc = spark.sparkContext

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            // Dataset
            val data = readDataFileByFilename(sc, testOptions.getDataFilePath(name).toString)
            val dimension = data.first()._2.size

            testOptions.ts.foreach { t =>
                println(s"===== $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"===== $k: ${new Date(System.currentTimeMillis())} =====")

                    val desiredSize = t * k

                    val baseDirectory = Paths.get(inputPath).resolve(s"$name/$t/$k")
                    Files.createDirectories(baseDirectory)

                    val result = Hasher.getHasherForDataset(data, dimension, desiredSize)
                    DataStore.kstore(
                        baseDirectory.resolve("hasher.dat"),
                        result)
                }
            }
        }
    }

    def readAllHashers(testOptions: TestOptions,
                       outputPath: String = "C:/result/hasher"): Map[(String, Int, Int), (Hasher, HashOptions, Double)] = {
        val map = Map[(String, Int, Int), (Hasher, HashOptions, Double)]()

        testOptions.datasets.foreach { name =>
            println(s"===== $name =====")

            testOptions.ts.foreach { t =>
                println(s"===== $t: ${new Date(System.currentTimeMillis())} =====")

                testOptions.ks.foreach { k =>
                    println(s"===== $k: ${new Date(System.currentTimeMillis())} =====")

                    val baseDirectory = Paths.get(outputPath).resolve(s"$name/$t/$k")

                    val result = DataStore.kload_v2[(Hasher, HashOptions, Double)](baseDirectory.resolve("hasher.dat"))
                    map += (name, t, k) -> result
                }
            }
        }
        map
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

    def initSpark(cores: String = "*"): SparkSession = {
        // https://dzone.com/articles/working-on-apache-spark-on-windows
        // https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
        // https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3
        // SPARK_HOME
        // HADOOP_HOME
        //System.setProperty("hadoop.home.dir", "C:/Users/joseluis/OneDrive/Aplicaciones/spark-3.0.1-hadoop-3.2")
        System.setProperty("hadoop.home.dir", "W:/spark/spark-3.0.1-hadoop-3.2")

        //System.setProperty("hadoop.home.dir", "C:/Users/joseluis/OneDrive/Aplicaciones/spark-2.4.7-hadoop-2.7")

        System.setProperty("spark.local.dir", "W:/spark/tmp2")

        // http://spark.apache.org/docs/latest/monitoring.html
        System.setProperty("spark.ui.port", "44041")

        Utils.quiet_logs()

        val spark = SparkSession.builder
            .master(s"local[$cores]")
            .appName("Simple Application")
            //.config("spark.driver.maxResultSize", "0")
            //.config("spark.driver.memory", "16g")
            //.config("spark.executor.memory", "32g")
            //.config("spark.driver.cores", "20")
            //.config("spark.driver.memory", "16g")
            //.config("spark.executor.cores", "20")
            //.config("spark.executor.memory", "16g")
            //.config("spark.master", "local")
            //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            //.config("spark.kryoserializer.buffer.max", "2000m") // 1024m,128m
            .getOrCreate()

        spark
    }

    def updateCores(sc: SparkSession, cores: String = "*"): Unit = {
        sc.conf.set("spark.master", s"local[$cores]")
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

    def readGroundTruthFileByFilename(sc: SparkContext, file: String): RDD[(Long, Array[Long])] = {
        readGroundTruthFile(sc, Paths.get(file))
    }

    def readGroundTruthFile(sc: SparkContext, file: Path): RDD[(Long, Array[Long])] = {
        val data: RDD[(Long, Array[Long])] = sc.textFile(file.toString)
            .zipWithIndex()
            .filter { case (line, index) => index > 0 }
            .map { case (line, index) => line }
            .map(line => {
                val args = line.split(',')
                val id = args(0).toLong
                val values = (1 until args.length).map(i => args(i).toLong).toArray
                (id, values)
            })
        data
    }

    def readApproxFileByFilename(sc: SparkContext, file: String): RDD[(Long, Array[Long])] = {
        readApproxFile(sc, Paths.get(file))
    }

    def readApproxFile(sc: SparkContext, file: Path): RDD[(Long, Array[Long])] = {
        val data: RDD[(Long, Array[Long])] = sc.textFile(file.toString)
            .zipWithIndex()
            .filter { case (line, index) => index > 0 }
            .map { case (line, index) => line }
            .map(line => {
                val args = line.split(',')
                val id = args(0).toLong
                val values = (1 until args.length).map(i => args(i).toLong).filter(id => id >= 0).toArray
                (id, values)
            })
        data
    }

    def readMaxDistancesFileByFilename(sc: SparkContext, file: String): RDD[(Long, Double)] = {
        readMaxDistancesFile(sc, Paths.get(file))
    }

    def readMaxDistancesFile(sc: SparkContext, file: Path): RDD[(Long, Double)] = {
        val data: RDD[(Long, Double)] = sc.textFile(file.toString)
            .zipWithIndex()
            .filter { case (line, index) => index > 0 }
            .map { case (line, index) => line }
            .map(line => {
                val args = line.split(',')
                val id = args(0).toLong
                val maxDistance = args(1).toDouble
                (id, maxDistance)
            })
        data
    }
}
