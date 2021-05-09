package org.example.evaluators

import org.apache.spark.ml.linalg.Vector

/**
 * Evaluador de Hash. Transforma el vector y después calcula el hash.
 */
trait TransformHashEvaluator extends HashEvaluator {

    def hash(point: Vector, radius: Double): Hash = hashTransformed(transform(point), radius)

    def transform(point: Vector): Seq[Double]

    def hashTransformed(transformed: Seq[Double], radius: Double): Hash
}
