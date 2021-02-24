package org.example.evaluators

import org.example.HashOptions

import scala.util.Random
import org.apache.spark.ml.linalg.Vector

class DefaultHasher(val evaluators: Array[HashEvaluator])
    extends Hasher {

    private def this() = this(new Array[HashEvaluator](0))

    def this(evaluators: HashEvaluator*) = this(evaluators.toArray);

    def this(random: Random, dim: Int, keyLength: Int, numTables: Int) = {
        this((0 until numTables).map(_ => new EuclideanHashEvaluator(random, dim, keyLength).asInstanceOf[HashEvaluator]).toArray);
    }

    def this(options: HashOptions) = {
        this((0 until options.numTables).map(_ => options.newHashEvaluator()).toArray);
    }

    def numTables: Int = evaluators.length;

    def hash(tableIndex: Int, point: Vector, radius: Double): HashPoint = {
        // Se incluye el índice en el hash
        new HashPoint(evaluators(tableIndex).hash(point, radius), tableIndex)
    }

    def tables: Seq[HashEvaluator] = evaluators

    private def firstHashEvaluator: Option[HashEvaluator] = evaluators.headOption;

    //def keyLength: Int = if (evaluators.nonEmpty) evaluators.head else 0;

    def hash(point: Vector, radius: Double): Seq[HashPoint] = {
        // Se incluye el índice en el hash
        evaluators.indices.map(index => new HashPoint(evaluators(index).hash(point, radius), index))
    }

    override def toString: String = {
        s"num_tables: $numTables " + firstHashEvaluator.map(_.toString);
    }
}
