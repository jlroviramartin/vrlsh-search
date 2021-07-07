package org.vrlshsearch.evaluators

import org.vrlshsearch.HashOptions

import org.apache.spark.ml.linalg.Vector
import scala.util.Random

/**
 * Simple line evaluator for a vector.
 */
class LineEvaluator(val w: Array[Double], val b: Double)
    extends Serializable {

    private def this() = this(new Array[Double](0), 0.0)

    def this(values: Double*) = {
        this((0 until values.size - 1).map(i => values(i)).toArray, values(values.size - 1))
    }

    def this(random: Random, dim: Int) = {
        this(
            (0 until dim).map(_ => random.nextGaussian).toArray,
            random.nextGaussian)
    }

    def this(options: HashOptions) = this(options.random, options.dim)

    def dimension: Int = w.length


    //def evaluate(point: Vector): Double = evaluate(point, 1)

    def evaluate(point: Vector, radius: Double): Double = {
        assert(point.size == dimension)
        ((0 until dimension).map(i => point(i) * w(i)).sum + b) / radius
        //(0 until dimension).map(i => point(i) * w(i)).sum / radius + b
    }

    def transform(point: Vector): Double = {
        (0 until dimension).map(i => point(i) * w(i)).sum + b
    }

    def hashTransformed(transformed: Double, radius: Double): Double = transformed / radius


    override def hashCode: Int = w.toSeq.hashCode() ^ b.hashCode()

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: LineEvaluator => w.toSeq.equals(other.w.toSeq) && b == other.b
            case _ => false
        }
    }
}
