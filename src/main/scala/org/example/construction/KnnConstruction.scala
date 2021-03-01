package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.Utils.time
import org.example.buckets.Bucket
import org.example.evaluators.{HashEvaluator, HashPoint, Hasher}
import org.example.{HashOptions, Utils}

import java.nio.file.Path
import scala.collection.{Iterable, mutable}

trait KnnConstruction extends Serializable {
    def build(data: RDD[(Long, Vector)])
}
