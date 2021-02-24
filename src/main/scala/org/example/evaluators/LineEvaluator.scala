package org.example.evaluators

import org.example.HashOptions

import org.apache.spark.ml.linalg.Vector
import scala.util.Random

/**
 * Simple line evaluator for a vector.
 */
class LineEvaluator(val w: Array[Double], val b: Double)
    extends Serializable {

    def this(values: Double*) = {
        this((0 until values.size - 1).map(i => values(i)).toArray, values(values.size - 1))
    }

    def this(random: Random, dim: Int) = {
        this(
            (0 until dim).map(_ => random.nextDouble).toArray, // nextGaussian
            random.nextDouble)
    }

    def this(options: HashOptions) = this(options.random, options.dim);

    def dimension: Int = w.length

    def evaluate(point: Vector): Double = evaluate(point, 1)

    def evaluate(point: Vector, radius: Double): Double = {
        assert(point.size == dimension)
        ((0 until dimension).map(i => point(i) * w(i)).sum + b) / radius;
    }
}
