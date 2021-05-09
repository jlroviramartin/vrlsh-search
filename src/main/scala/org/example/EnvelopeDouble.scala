package org.example

import org.apache.spark.ml.linalg.{Vector, Vectors}

import java.util.Arrays
import Array._

class EnvelopeDouble(
                        val min: Array[Double],
                        val max: Array[Double]) extends Serializable {

    def this(dimension: Int) = this(ofDim[Double](dimension), ofDim[Double](dimension))

    def this(min: Iterable[Double], max: Iterable[Double]) = this(min.toArray, max.toArray)

    def this(points: Iterable[Double]) = this(points.toArray, points.toArray)

    def this(point: Vector) = this(point.toArray, point.toArray)

    def this(points: Double*) = this(points, points)

    def dimension: Int = min.length

    def indices: Range = min.indices

    def minimum: Seq[Double] = min

    def maximum: Seq[Double] = max

    def sizes: Seq[Double] = min.zip(max).map { case (min, max) => max - min }

    def maxDistance: Double = Math.sqrt(min.zip(max).map { case (min, max) => (max - min) * (max - min) }.sum)

    def maxDistance(distanceEvaluator: KnnDistance): Double = distanceEvaluator.distance(Vectors.dense(min), Vectors.dense(max))

    def join(point: Seq[Double]): EnvelopeDouble = {
        if (dimension == 0) {
            new EnvelopeDouble(point)
        } else {
            assert(dimension == point.length)
            new EnvelopeDouble(
                min.indices.map(i => Math.min(min(i), point(i))),
                max.indices.map(i => Math.max(max(i), point(i))))
        }
    }

    def join(point: Vector): EnvelopeDouble = {
        if (dimension == 0) {
            new EnvelopeDouble(point)
        } else {
            assert(dimension == point.size)
            new EnvelopeDouble(
                min.indices.map(i => Math.min(min(i), point(i))),
                max.indices.map(i => Math.max(max(i), point(i))))
        }
    }

    def join(envelope: EnvelopeDouble): EnvelopeDouble = {
        if (dimension == 0) {
            envelope
        } else if (envelope.dimension == 0) {
            this
        } else {
            assert(dimension == envelope.dimension)
            new EnvelopeDouble(
                min.indices.map(i => Math.min(min(i), envelope.min(i))),
                max.indices.map(i => Math.max(max(i), envelope.max(i))))
        }
    }

    override def hashCode: Int = Arrays.hashCode(min) ^ Arrays.hashCode(max)

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: EnvelopeDouble => Arrays.equals(min, other.min) && Arrays.equals(max, other.max)
            case _ => false
        }
    }

    override def toString: String = s"[ ${Arrays.toString(min)} ; ${Arrays.toString(max)} ]"
}

object EnvelopeDouble {
    val EMPTY = new EnvelopeDouble(0)

    def seqOp(accumulator: EnvelopeDouble, point: Vector): EnvelopeDouble = accumulator.join(point)

    def combOp(accumulator1: EnvelopeDouble, accumulator2: EnvelopeDouble): EnvelopeDouble = accumulator1.join(accumulator2)

    def normalize(envelope: EnvelopeDouble, point: Vector): Vector = {
        val v = point.toDense
        Vectors.dense(
            envelope.indices.map(i => {
                (v(i) - envelope.min(i)) / (envelope.max(i) - envelope.min(i))
            }).toArray)
    }
}