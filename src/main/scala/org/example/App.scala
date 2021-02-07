package org.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object App {

    def foo(x: Array[String]): String = x.foldLeft("")((a, b) => a + b)

    def main(args: Array[String]) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\joseluis\\OneDrive\\Aplicaciones\\spark-3.0.1-hadoop-3.2")

        println("Hello World!")
        println("concat arguments = " + foo(args))

        val conf = new SparkConf()
            .setAppName("ScalaApp")
            .setMaster("local[2]")
            .set("spark.executor.memory", "1g")
            .set("spark.ui.port", "54048")

        val sc = new SparkContext(conf)
    }
}
