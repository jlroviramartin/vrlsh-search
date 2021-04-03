package org.example.construction

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector

import scala.collection.immutable.Iterable

trait KnnQuery {
    def query(point: Vector, k: Int, statistics: StatisticsCollector): Iterable[(Double, Long)]

    /**
     * Permite serializar la consulta y los datos.
     */
    def getSerializable(): KnnQuerySerializable

    def printResume()
}

trait KnnQuerySerializable extends Serializable {

    def get(sc: SparkContext): KnnQuery
}
