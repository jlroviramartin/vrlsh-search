package org.example.evaluators

import org.apache.spark.ml.linalg.Vector

/**
 * Evaluador de Hash. Transforma el vector y después calcula el hash.
 */
trait TransformHashEvaluator extends HashEvaluator {

    final def hash(point: Vector, radius: Double): HashPoint = hashTransformed(transform(point), radius);

    def transform(point: Vector): Vector;

    def hashTransformed(point: Vector, radius: Double): HashPoint;
}
