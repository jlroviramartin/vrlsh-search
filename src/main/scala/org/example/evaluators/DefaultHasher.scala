package org.example.evaluators

import org.example.HashOptions

import scala.util.Random
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

import scala.Array.ofDim

class DefaultHasher(val evaluators: Array[HashEvaluator])
    extends Hasher {

    private def this() = this(new Array[HashEvaluator](0))

    def this(evaluators: HashEvaluator*) = this(evaluators.toArray);

    def this(random: Random, dim: Int, keyLength: Int, numTables: Int) = {
        this((0 until numTables).map(_ => new EuclideanHashEvaluator(random, dim, keyLength)).toArray[HashEvaluator]);
    }

    def this(options: HashOptions) = {
        this((0 until options.numTables).map(_ => options.newHashEvaluator()).toArray);
    }

    def numTables: Int = evaluators.length;

    def hash(tableIndex: Int, point: Vector, radius: Double): Hash = {
        // Se incluye el índice en el hash
        //new Hash(evaluators(tableIndex).hash(point, radius), tableIndex)
        //evaluators(tableIndex).hash(point, radius)
        new HashWithIndex(tableIndex, evaluators(tableIndex).hash(point, radius))
    }

    //def tables: Seq[HashEvaluator] = evaluators

    private def firstHashEvaluator: Option[HashEvaluator] = evaluators.headOption;

    //def keyLength: Int = if (evaluators.nonEmpty) evaluators.head else 0;

    def hash(point: Vector, radius: Double): Seq[Hash] = {
        evaluators.indices.map(index => hash(index, point, radius))
    }

    override def toString: String = {
        s"num_tables: $numTables " + firstHashEvaluator.map(_.toString);
    }

    override def hashCode: Int = evaluators.toSeq.hashCode

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: DefaultHasher => evaluators.toSeq.equals(other.evaluators.toSeq)
            case _ => false
        }
    }
}

class EuclideanHasher(val table: Array[Array[Array[Double]]],
                      val b: Array[Array[Double]],
                      val numTables: Int,
                      val keyLength: Int,
                      val dimension: Int)
    extends Hasher {

    def this(options: HashOptions) = {
        this(ofDim[Double](options.numTables, options.keyLength, options.dim),
            ofDim[Double](options.numTables, options.keyLength),
            options.numTables, options.keyLength, options.dim);

        for (i <- 0 until numTables)
            for (j <- 0 until options.keyLength) {
                for (k <- 0 until options.dim)
                    table(i)(j)(k) = options.random.nextGaussian
                b(i)(j) = options.random.nextGaussian
            }
    }

    override def hash(point: Vector, radius: Double): Seq[Hash] = {
        (0 until numTables).map(index => {
            hash(index, point, radius)
        })
    }

    override def hash(tableIndex: Int, point: Vector, radius: Double): Hash = {
        val hash = (0 until keyLength).map(j => {
            var dotProd = 0.0

            point match {
                case dense: DenseVector => {
                    for (k <- 0 until dimension)
                        dotProd += dense(k) * table(tableIndex)(j)(k)
                }
                case sparse: SparseVector => { //SparseVector
                    val indices = sparse.indices
                    val values = sparse.values

                    for (k <- 0 until indices.length) {
                        dotProd += values(k) * table(tableIndex)(j)(indices(k))
                    }
                }
            }
            //dotProd /= radius
            math.floor((dotProd + b(tableIndex)(j)) / radius).toInt
        }).toArray
        new HashWithIndex(tableIndex, new HashPoint(hash))
    }

    //override def tables: Seq[HashEvaluator] =
}
