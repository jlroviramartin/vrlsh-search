package org.example.evaluators

import org.example.HashOptions
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.util.Random

/**
 * Hash evaluator for a vector.
 * Mapea vectores: dim -> alfa
 */
class EuclideanHashEvaluator(evaluators: Seq[LineEvaluator])
    extends TransformHashEvaluator with Serializable {

    def this(random: Random, dim: Int, keyLength: Int) = this((0 until keyLength).map(_ => new LineEvaluator(random, dim)));

    def this(options: HashOptions) = this(options.random, options.dim, options.keyLength);

    def keyLength: Int = evaluators.length;

    def dimension: Int = if (evaluators.nonEmpty) evaluators.head.dimension else 0;

    def transform(point: Vector): Vector = Vectors.dense(evaluators.map(evaluator => evaluator.evaluate(point)).toArray);

    def hashTransformed(point: Vector, resolution: Double): HashPoint = new HashPoint(point.toArray.map(x => (x * resolution).toInt));

    override def toString: String = {
        "keyLength: " + keyLength + " dimension: " + dimension;
    }
}
