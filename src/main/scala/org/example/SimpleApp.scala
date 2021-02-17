package org.example

import Utils._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vectors._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.example.buckets.Bucket
import org.example.evaluators.{DefaultHasher, EuclideanHashEvaluator, Hasher}

import scala.util.Random;

object SimpleApp {
    def main(args: Array[String]) {
        // https://dzone.com/articles/working-on-apache-spark-on-windows
        // https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
        // https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3
        // SPARK_HOME
        // HADOOP_HOME
        //System.setProperty("hadoop.home.dir", "C:\\Users\\joseluis\\OneDrive\\Aplicaciones\\spark-3.0.1-hadoop-3.2")
        System.setProperty("hadoop.home.dir", "C:\\Users\\joseluis\\OneDrive\\Aplicaciones\\spark-2.4.7-hadoop-2.7")

        // http://spark.apache.org/docs/latest/monitoring.html
        System.setProperty("spark.ui.port", "44041")

        //val fileToRead = "hdfs://namenode:9000/test.txt"
        //val fileToRead = "C:\\Temp\\test.csv"
        val fileToRead = "C:\\Users\\joseluis\\OneDrive\\TFM\\HIGGS\\HIGGS_head_numbered_10.csv"

        val spark = SparkSession.builder
            .appName("Simple Application")
            .config("spark.master", "local")
            .getOrCreate()
        val sc = spark.sparkContext

        val data = sc.textFile(fileToRead)
            .map(line => {
                val args = line.split(',');
                val index = args(0).toLong;
                val values = (1 until args.length).map(i => args(i).toDouble).toArray;
                (index, new LabeledPoint(0.0, Vectors.dense(values)))
            })

        /*
        // https://stackoverflow.com/a/61100197
        import spark.implicits._

        // https://stackoverflow.com/a/44774366
        //import spark.sqlContext.implicits._

        val lines = spark.read
            .format("csv")
            .option("header", "false")
            .option("delimiter", ",")
            .option("inferSchema", "true")
            .csv(fileToRead)
        val columns = lines.columns

        lines.map(
            line => {
                val index = line.getInt(0)
                val data = (1 until line.size).map(i => line.getDouble(i)).toArray
                (index, Vectors.dense(data))
            })*/
        //new PointProcessor(options).process(ds)

        val dim = data.first()._2.features.size
        //val dim = lines.columns.length

        val options = new HashOptions(
            random = new Random(0),
            dim = dim,
            keyLength = 2,
            numTables = 2
        );

        val hasher = new DefaultHasher(options)
        val resolution = 0.1

        val minValue = 0.1;
        val dimension = data.first()._2.features.size;
        val desiredSize = 10;

        val radius = Hasher.getHasherForDataset(data, dimension, desiredSize);

        val processed = data
            .groupBy(line => {
                val point = line._2.features
                hasher.hash(point, resolution)
            })
            .map(t => {
                val hash = t._1
                val values = t._2

                val bucket = new Bucket(options)
                values.map(value => value._2.features)
                    .foreach(v => bucket.put(v))
                Console.println("bucket " + hash + " : " + bucket.size)

                (resolution, hash, bucket)
            })

        val unique = processed.map { case (resolution, hash, bucket) => hash }.distinct().collect()

        processed.saveAsTextFile("C:\\scala-output\\files\\")

        spark.stop()
    }
}
