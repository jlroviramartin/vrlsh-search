package org.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors._
import Utils._;

object SimpleApp {
    def main(args: Array[String]) {
        // https://dzone.com/articles/working-on-apache-spark-on-windows
        // https://stackoverflow.com/questions/35652665/java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-in-the-ha
        // https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3
        // SPARK_HOME
        // HADOOP_HOME
        System.setProperty("hadoop.home.dir", "C:\\Users\\joseluis\\OneDrive\\Aplicaciones\\spark-3.0.1-hadoop-3.2")

        // http://spark.apache.org/docs/latest/monitoring.html
        System.setProperty("spark.ui.port", "44041")

        //val fileToRead = "hdfs://namenode:9000/test.txt"
        //val fileToRead = "C:\\Temp\\test.csv"
        val fileToRead = "C:\\Users\\joseluis\\Downloads\\HIGGS\\HIGGS_head.csv"

        val spark = SparkSession.builder
            .appName("Simple Application")
            .config("spark.master", "local")
            .getOrCreate()

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

        val options = new HashOptions(
            dim = columns.length,
            alfa = 2,
            beta = 2
        );

        val useIndex = false;
        val ds =
            if (useIndex)
                lines.map(
                    line => {
                        val index = line.getInt(0)
                        val data = (1 until line.size).map(i => line.getDouble(i)).toArray
                        (index, Vectors.dense(data))
                    })
            else
                lines.map(
                    line => {
                        val data = (0 until line.size).map(i => line.getDouble(i)).toArray
                        (0, Vectors.dense(data))
                    })
        //new PointProcessor(options).process(ds)

        ds.map { case k: (Int, Vector) => k._2}

        spark.stop()
    }
}
