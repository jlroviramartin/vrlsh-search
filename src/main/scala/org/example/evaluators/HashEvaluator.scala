package org.example.evaluators

import org.apache.spark.mllib.linalg.Vector

/**
 * Evaluador de Hash.
 */
trait HashEvaluator {

    def hash(v: Seq[Double], resolution: Double): HashPoint;

    def hash(v: Vector, resolution: Double): HashPoint;
}
