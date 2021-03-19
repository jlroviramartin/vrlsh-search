package org.example

import Utils._

import collection._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vectors._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.example.buckets.Bucket
import org.example.construction.{KnnConstructionAlgorithm, KnnMetadata, KnnQuery, KnnQuerySerializable, MyKnnQuery, MyKnnQuerySerializator}
import org.example.evaluators.{DefaultHasher, EuclideanHashEvaluator, EuclideanHasher, Hash, HashEvaluator, Hasher, LineEvaluator, TransformHashEvaluator}

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.reflect.io.Directory
import scala.util.Random;

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

        val desiredSize = 100

        // Se limpian los datos antiguos
        //val directory = new Directory(baseDirectory.resolve("sizes").toFile)
        //directory.deleteRecursively()

        val envelope = data
            .map { case (_, point) => point }
            .aggregate(EnvelopeDouble.EMPTY)(
                EnvelopeDouble.seqOp,
                EnvelopeDouble.combOp)
        println(s"Envelope: $envelope")
        println(s"Min: ${envelope.sizes.min} Max: ${envelope.sizes.max}")

        // Build
        val knnQuery: KnnQuery = if (false) {
            val distance = new KnnEuclideanDistance
            val knnQuery = time {
                // Se limpian los datos antiguos
                val directory = new Directory(baseDirectory.toFile)
                directory.deleteRecursively()

                new KnnConstructionAlgorithm(desiredSize, baseDirectory.toString, distance).build(data)
            }

            DataStore.kstore(
                baseDirectory.resolve("KnnQuery.dat"),
                knnQuery.getSerializable())

            knnQuery
        } else {
            val knnQuery = DataStore.kload(
                baseDirectory.resolve("KnnQuery.dat"),
                classOf[MyKnnQuerySerializator]).get(sc)
            knnQuery
        }


        val k = 10

        val sample = data.takeSample(withReplacement = false, 1)
        val query = sample(0)._2

        check(data, knnQuery, query, k)

        /*val distance = new KnnEuclideanDistance

        println("===== Fuerza 'bruta'  =====")

        val result = data
            .map { case (id, point) => (distance.distance(query, point), id) }
            .aggregate(new KnnResult())(KnnResult.seqOp(kFuerzaBruta), KnnResult.combOp(kFuerzaBruta))
            .sorted.toList

        println(s"Query: $iquery")
        println("Resultado:")
        result.foreach { case (distance, id) => println(s"  $distance - $id") }

        println(s"===== Se ejecuta el algoritmo =====")

        val knnQuery = time {
            // Se limpian los datos antiguos
            val directory = new Directory(baseDirectory.toFile)
            directory.deleteRecursively()

            new KnnConstructionAlgorithm(desiredSize, baseDirectory.toString, distance).build(data)
        }

        val result2 = knnQuery.query(query, k)
        println("Resultado:")
        result2.foreach { case (distance, id) => println(s"  $distance - $id") }*/

        spark.stop()
    }

    val random = new Random(0)

    def random(envelope: EnvelopeDouble): Vector = {
        Vectors.dense(envelope.min.zip(envelope.max).map { case (min, max) => min + random.nextDouble() * (max - min) })
    }

    def check(data: RDD[(Long, Vector)], knnQuery: KnnQuery, query: Vector, k: Int): Unit = {
        println(s"Query: $query")

        println("===== Algorithm  =====")

        val result = knnQuery.query(query, k)

        println("===== Fuerza 'bruta'  =====")
        val kFuerzaBruta = 1000

        val distance = new KnnEuclideanDistance

        val realMap = data
            .map { case (id, point) => (distance.distance(query, point), id) }
            .aggregate(new KnnResult())(KnnResult.seqOp(kFuerzaBruta), KnnResult.combOp(kFuerzaBruta))
            .sorted
            .zipWithIndex
            .map { case ((d, id), index) => (id, (index, d)) }
            .toMap

        //realMap
        //    .foreach { case (id, (index, d)) => println(s"  $d - $id - $index") }

        result
            .map { case (d, id) => (id, d, realMap(id)._1) }
            .foreach { case (id, d, index) => println(s"  $d - $id - $index") }
    }
}
