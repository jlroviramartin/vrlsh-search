package org.example.evaluators

import org.example.HashOptions

import scala.util.Random
import org.apache.spark.ml.linalg.Vector

class DefaultHasher(val evaluators: Array[HashEvaluator])
    extends Hasher {

    def this(random: Random, dim: Int, keyLength: Int, numTables: Int) = {
        this((0 until numTables).map(_ => new EuclideanHashEvaluator(random, dim, keyLength).asInstanceOf[HashEvaluator]).toArray);
    }

    def this(options: HashOptions) = {
        this((0 until options.numTables).map(_ => options.newHashEvaluator()).toArray);
    }

    def numTables: Int = evaluators.length;

    private def firstHashEvaluator: Option[HashEvaluator] = if (evaluators.nonEmpty) Some(evaluators.head) else None;

    //def keyLength: Int = if (evaluators.nonEmpty) evaluators.head else 0;

    def hash(v: Vector, resolution: Double): Seq[HashPoint] = {
        // Se incluye el índice en el hash
        evaluators.indices.map(index => new HashPoint(evaluators(index).hash(v, resolution), index))
    }

    override def toString: String = {
        "num_tables:" + numTables + firstHashEvaluator.map(" " + _.toString);
    }
}
