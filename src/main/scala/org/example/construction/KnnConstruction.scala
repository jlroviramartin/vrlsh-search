package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

trait KnnConstruction extends Serializable {
    def build(data: RDD[(Long, Vector)]): KnnQuery
}
