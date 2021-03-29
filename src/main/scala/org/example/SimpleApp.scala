package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import java.nio.file.{Files, Paths}

// How to turn off INFO logging in Spark? https://stackoverflow.com/a/26123496
object SimpleApp {
    def main(args: Array[String]) {
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

        //val fileToRead = "hdfs://namenode:9000/test.txt"
        //val fileToRead = Paths.get("C:", "Users", "joseluis", "OneDrive", "TFM", "dataset", "HIGGS_head_numbered_100000.csv")
        val fileToRead = Paths.get("C:", "Users", "joseluis", "OneDrive", "TFM", "dataset", "corel.csv")

        val baseDirectory = Paths.get("C:", "Temp", "scala")
        Files.createDirectories(baseDirectory)

        val spark = SparkSession.builder
            .appName("Simple Application")
            .config("spark.master", "local")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer", "64m")
            .config("spark.kryoserializer.buffer.max", "128m")
            .getOrCreate()
        val sc = spark.sparkContext


        /*
        val result = sc
            .textFile(Paths.get("C:", "Users", "joseluis", "OneDrive", "TFM", "dataset", "shape_NonIndexed.csv").toString)
            .randomSplit(Array(0.9, 0.1), seed = 12345)
        result(0)
            .repartition(1)
            .saveAsTextFile(Paths.get("C:", "Users", "joseluis", "OneDrive", "TFM", "dataset", "shape_NonIndexed.csv_p9.csv").toString)
        result(1)
            .repartition(1)
            .saveAsTextFile(Paths.get("C:", "Users", "joseluis", "OneDrive", "TFM", "dataset", "shape_NonIndexed.csv_p1.csv").toString)
        */


        val data: RDD[(Long, Vector)] = if (true) {
            sc.textFile(fileToRead.toString)
                .map(line => {
                    val args = line.split(',')
                    val id = args(0).toLong
                    val values = (1 until args.length).map(i => args(i).toDouble).toArray
                    (id, Vectors.dense(values))
                })
        } else {
            // No funciona ???
            MLUtils.loadLibSVMFile(sc, fileToRead.toString)
                .zipWithIndex()
                .map(_.swap)
                .map { case (id, labelPoint) => (id, Vectors.dense(labelPoint.features.toArray)) }
        }

        // Se calcula el recubrimiento
        val envelope = data
            .map { case (_, point) => point }
            .aggregate(EnvelopeDouble.EMPTY)(
                EnvelopeDouble.seqOp,
                EnvelopeDouble.combOp)

        val desiredSize = 100

        KnnTest.testSet1(envelope, data.cache(), baseDirectory, desiredSize)
        //KnnTest.testSet2(data, baseDirectory, desiredSize, 10, 10)
        spark.stop()
    }
}
