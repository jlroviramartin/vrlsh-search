package org.example.evaluators

import org.example.HashOptions

import org.apache.spark.ml.linalg.Vector
import scala.util.Random

/**
 * Simple line evaluator for a vector.
 */
class LineEvaluator(val w: Seq[Double], val b: Double)
    extends Serializable {

    def this(random: Random, dim: Int) = {
        this(
            (0 until dim).map(_ => random.nextDouble()),
            random.nextDouble())
    }

    def this(options: HashOptions) = this(options.random, options.dim);

    def dimension: Int = w.length

    def evaluate(point: Vector): Double = evaluate(point, 1)

    def evaluate(point: Vector, resolution: Double): Double = {
        assert(point.size == dimension)
        ((0 until dimension).map(i => point(i) * w(i)).sum + b) * resolution;
    }
}
