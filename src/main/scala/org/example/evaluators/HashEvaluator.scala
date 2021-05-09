package org.example.evaluators

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Evaluador de Hash.
 */
trait HashEvaluator extends Serializable {

    def hash(v: Vector, radius: Double): Hash

    final def hashData(data: RDD[(Long, Vector)], radius: Double): RDD[(Long, Hash)] = {
        val bthis = data.sparkContext.broadcast(this)

        data.map({ case (id, point) => (id, bthis.value.hash(point, radius)) })
    }

    final def sizesByHash(data: RDD[(Long, Vector)],
                          radius: Double): RDD[(Hash, Int)] = {

        this.hashData(data, radius) // (id, point) -> (id, hash)
            .map(_.swap) // (id, hash) -> (hash, id)
            .aggregateByKey(0)( // Se cuenta el número de puntos en cada bucket
                { case (numPts, id) => numPts + 1 },
                { case (numPts1, numPts2) => numPts1 + numPts2 })
    }

    final def sizesStatistics(data: RDD[(Long, Vector)],
                              radius: Double): RDD[(Int, Int)] = {

        this.sizesByHash(data, radius) // (id, point) -> (id, hash)
            .map({ case (hash, numPts) => (numPts, 1) })
            .reduceByKey((count1, count2) => count1 + count2) // Se cuentan los buckets con el mismo número de puntos
    }
}
