package org.example.testing

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.KnnDistance
import org.example.construction.KnnQuery
import org.example.testing.TestingUtils._

class KnnTest_v2 {
    def doTest(knnQuery: KnnQuery,
               distance: KnnDistance,
               data: RDD[(Long, Vector)],
               test: List[Vector]) = {
    }
}
