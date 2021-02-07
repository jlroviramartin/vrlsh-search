package org.example.evaluators

import org.apache.spark.mllib.linalg.Vector

/**
 * Evaluador de Hash.
 */
trait TrHashEvaluator extends HashEvaluator {

    def hash(point: Seq[Double], resolution: Double): HashPoint = trHash(transform(point), resolution);

    def hash(point: Vector, resolution: Double): HashPoint = trHash(transform(point), resolution);

    def transform(point: Seq[Double]): Seq[Double];

    def transform(point: Vector): Seq[Double];

    def trHash(point: Seq[Double], resolution: Double): HashPoint;
}
