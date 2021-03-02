package org.example.evaluators

import org.example.HashOptions
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.util.Random

/**
 * Hash evaluator for a vector.
 * Mapea vectores: dim -> alfa
 */
class EuclideanHashEvaluator(val evaluators: Seq[LineEvaluator])
    extends TransformHashEvaluator with Serializable {

    private def this() = this(Seq.empty[LineEvaluator])

    def this(random: Random, dim: Int, keyLength: Int) = this((0 until keyLength).map(_ => new LineEvaluator(random, dim)));

    def this(options: HashOptions) = this(options.random, options.dim, options.keyLength);

    def keyLength: Int = evaluators.length;

    def dimension: Int = if (evaluators.nonEmpty) evaluators.head.dimension else 0;


    override def hash(point: Vector, radius: Double): Hash =
        new HashPoint(evaluators.map(evaluator => evaluator.evaluate(point, radius).toInt));

    def transform(point: Vector): Seq[Double] =
        evaluators.map(evaluator => evaluator.transform(point));

    def hashTransformed(transformed: Seq[Double], radius: Double): Hash =
        new HashPoint(evaluators.zip(transformed)
            .map { case (evaluator, t) => Math.floor(evaluator.hashTransformed(t, radius)).toInt });


    override def toString: String = s"keyLength: $keyLength  dimension: $dimension";

    override def hashCode: Int = evaluators.hashCode()

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: EuclideanHashEvaluator => evaluators.equals(other.evaluators)
            case _ => false
        }
    }
}
