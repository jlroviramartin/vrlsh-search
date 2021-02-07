package org.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.linalg.Vector

// SCALA Extension methods: https://sachabarbs.wordpress.com/2015/10/23/scala-extension-methods/
class PointProcessor(val options: HashOptions) {

    def process(source: Dataset[(Int, Vector)]): Unit = {
        val buckets = options.newTrBuckets();
        val resolution = 0.1;
        source.foreach(line => buckets.put(line._2, resolution));

        buckets.getBuckets(resolution).foreach(kv => {
            Console.println("bucket " + kv._1 + " : " + kv._2.points.size)
        })
    }
}
