package org.example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object App {

    def foo(x: Array[String]): String = x.foldLeft("")((a, b) => a + b)

    def main(args: Array[String]) {

        var list1= List(100, 400, 200, 500, 100, 1900, 2000, 400, 400, 19000)
        println("list before group by is  ::")
        println(list1)
        //applying groupBy method here
        var group1 = list1.groupBy(x => x)
        // printing output
        println("list after group by is  ::")
        println(group1)


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
