package org.example.evaluators

import org.example.HashOptions

import org.apache.spark.mllib.linalg.Vector
import scala.util.Random

/**
 * Hash evaluator for a vector.
 * * Mapea vectores: dim -> alfa
 */
class VectorHashEvaluator(evaluators: Seq[LineEvaluator])
    extends TrHashEvaluator with Serializable {

    def this(random: Random, dim: Int, alfa: Int) = this((0 until alfa).map(_ => new LineEvaluator(random, dim)))

    def this(options: HashOptions) = this(options.random, options.dim, options.alfa);

    def alfa: Int = evaluators.length;

    def dimension: Int = {
        if (evaluators.nonEmpty)
            evaluators.head.dimension
        else
            0;
    }

    def transform(point: Seq[Double]): Seq[Double] = {
        evaluators.map(evaluator => evaluator.evaluate(point));
    }

    def transform(point: Vector): Seq[Double] = {
        evaluators.map(evaluator => evaluator.evaluate(point));
    }

    def trHash(point: Seq[Double], resolution: Double): HashPoint = {
        new HashPoint(point.map(x => (x * resolution).toInt))
    }
}
