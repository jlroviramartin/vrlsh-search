package org.example.evaluators

import org.apache.spark.ml.linalg.Vector

/**
 * Evaluador de Hash.
 */
trait HashEvaluator extends Serializable {
    def hash(v: Vector, resolution: Double): HashPoint;
}
