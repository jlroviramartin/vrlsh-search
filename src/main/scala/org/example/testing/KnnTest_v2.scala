package org.example.testing

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.KnnDistance
import org.example.construction.KnnQuery

class KnnTest_v2 {
    def doTest(knnQuery: KnnQuery,
               distance: KnnDistance,
               data: RDD[(Long, Vector)],
               test: RDD[(Vector)]) = {
    }
}
