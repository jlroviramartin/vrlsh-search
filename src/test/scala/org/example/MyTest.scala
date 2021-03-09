package org.example

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class MyTest extends AnyFunSuite {
    test("Line evaluator") {
        val spark = SparkSession.builder
            .appName("Simple Application")
            .config("spark.master", "local")
            .getOrCreate()
        val sc = spark.sparkContext

        val rdd = sc.makeRDD(Array((1, 2), (3, 4), (3, 6)))
        val other = sc.makeRDD(Array((3, 9)))

        rdd.subtractByKey(other).collect()
    }
}
