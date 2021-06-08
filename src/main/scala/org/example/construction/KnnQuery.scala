package org.example.construction

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.example.KnnDistance
import org.example.statistics.StatisticsCollector

import scala.collection.immutable.Iterable

trait KnnQuery {
    def query(point: Vector,
              k: Int,
              distanceEvaluator: KnnDistance,
              statistics: StatisticsCollector): Iterable[(Double, Long)]

    /**
     * Permite serializar la consulta y los datos.
     */
    def getSerializable(): KnnQuerySerializable

    //def printResume()
}

trait KnnQuerySerializable extends Serializable {

    def get(sc: SparkContext): KnnQuery
}
