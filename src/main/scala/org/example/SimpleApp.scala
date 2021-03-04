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
import org.example.construction.{KnnConstructionAlgorithm, KnnConstructionAlgorithm_v2}
import org.example.evaluators.{DefaultHasher, EuclideanHashEvaluator, Hash, HashEvaluator, Hasher, LineEvaluator, TransformHashEvaluator}

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
                    val args = line.split(',');
                    val id = args(0).toLong;
                    val values = (1 until args.length).map(i => args(i).toDouble).toArray;
                    (id, Vectors.dense(values))
                })
        } else {
            // No funciona ???
            MLUtils.loadLibSVMFile(sc, fileToRead.toString)
                .zipWithIndex()
                .map(_.swap)
                .map { case (id, labelPoint) => (id, Vectors.dense(labelPoint.features.toArray)) }
        }

        val desiredSize = 100;

        // Se limpian los datos antiguos
        val directory = new Directory(baseDirectory.resolve("sizes").toFile);
        directory.deleteRecursively();

        time {
            //new KnnConstructionAlgorithm(desiredSize, baseDirectory.toString).build(data)
            new KnnConstructionAlgorithm_v2(desiredSize, baseDirectory.toString).build(data)
        }
        spark.stop()
    }

    def algorithmv1(data: RDD[(Long, Vector)], dimension: Int, desiredSize: Int, baseDirectory: Path, depth: Int = 0): Unit = {

        if (depth > 3) {
            return;
        }

        println("----- Calculando Hasher -----")
        val (hasher, hashOptions, currentRadius) = time { // 3:45 min
            Hasher.getHasherForDataset(data, dimension, desiredSize);
        }
        var radius = currentRadius

        //Utils.store(baseDirectory.resolve("hasher.data"), (hasher, radius))
        //val (hasher, radius) = Utils.load[(Hasher, Double)](baseDirectory.resolve("hasher.data"))

        for (i <- 1 to 3) {
            println(s"----- Iteración $i -----")

            println("----- Estadísticas -----")
            time {
                hasher.sizesStatistics(data, radius)
                    .foreach { case (numPoints, count) =>
                        println(s"$numPoints * $count");
                    }
            }

            val grouped =
                hasher.hashDataWithVectors(data, radius)
                    .groupByKey
                    .map { case (hash, it) => it };

            println("----- Almacena los buckets -----")
            time {
                grouped
                    .filter(it => it.size >= 90)
                    .map(it => new Bucket(it.map { case (id, point) => point }))
                    .saveAsObjectFile(baseDirectory.resolve(Paths.get("sizes", s"$radius")).toString)
            }

            println("----- Puntos restantes -----")
            val remaining =
                grouped
                    .filter(it => it.size < 90)
                    .flatMap(it => it)


            // DEBUG
            time {
                println(s"remaining = ${remaining.count()}")
            }

            if (remaining.isEmpty()) {
                // Finaliza
                return;
            }

            println("----- Se itera -----")
            //algorithm1(remaining, dimension, desiredSize, baseDirectory, depth + 1)

            radius = radius * 1.5
        }
    }

    def pruebas(data: RDD[(Long, Vector)], dimension: Int, radius: Double, desiredSize: Int): Unit = {
        val (hasher, hashOptions, radius) = time { // 3:45 min
            Hasher.getHasherForDataset(data, dimension, desiredSize);
        }
        //Utils.store(baseDirectory.resolve("hasher.data"), (hasher, radius))
        //val (hasher, radius) = Utils.load[(Hasher, Double)](baseDirectory.resolve("hasher.data"))

        println("TEST 1"); // 4:22 min
        time {
            hasher.hashDataWithVectors(data, radius)
                .aggregateByKey(List[Vector]())( // Se cuenta el número de puntos en cada bucket
                    { case (agg, id) => agg ++ List(id._2) },
                    { case (agg1, agg2) => agg1 ++ agg2 })
                .filter { case (hash, agg) => agg.size > 90 }
                .foreach { case (hash, agg) => {
                    println(s"----------> ${agg.size}");
                }
                }
        }

        println("TEST 2"); // 4:00 min
        time {
            hasher.sizesByHash(data, radius)
                .filter { case (hash, numPoints) => numPoints > 90 }
                .foreach { case (hash, numPoints) =>
                    println(s"$numPoints <- $hash");
                }
        }
        println("TEST 3"); // 3:24 min / 2:32 min
        time {
            hasher.hashDataWithVectors(data, radius)
                .groupByKey
                .filter { case (hash, it) => it.size > 90 }
                .foreach { case (hash, it) =>
                    println(s"${it.size} <- $hash");
                }
        }

        println("Estadísticas: Número de puntos - Número de buckets");

        println("TEST 4"); // 3:08 min
        time {
            hasher.sizesStatistics(data, radius)
                .foreach { case (numPoints, count) =>
                    println(s"$numPoints * $count");
                }
        }

        // ????
        val c = data
            .groupBy { case (_, point) => hasher.hash(point, radius) }
            .filter { case (_, it) => equalsWithTolerance(it, desiredSize) }
            .map { case (h, it) => (h, new Bucket(it.map { case (_, point) => point })) }
            .count()
        println(s"Total $c");

        //bucketData
        //    .map { case (h, bucket) => (h, bucket.points.size) }
        //    .saveAsTextFile(baseDirectory.resolve("sizes").resolve("" + radius));

        //bucketData
        //    .filter { case (_, bucket) => equalsWithTolerance(bucket.size, desiredSize) }
        //    //.saveAsTextFile("C:\\Temp\\" + radius)
        //    .foreach { case (h, bucket) => {
        //        println(s"$h")
        //        println("==================================================")
        //        println(s"$bucket")
        //    }}

        //bucketData
        //    .filter { case (_, bucket) => !equalsWithTolerance(bucket.size, desiredSize) }
        //    .saveAsTextFile("C:\\Temp\\OTHER_" + radius)
    }
}
