package org.vrlshsearch

import org.apache.spark.ml.linalg.Vector

import java.util.Arrays

class EnvelopeDoubleBuffer(val min: Array[Double], val max: Array[Double]) extends Serializable {

    def this(dimension: Int) = {
        this(new Array[Double](dimension), new Array[Double](dimension));
        setEmpty();
    }

    def this(min: Iterable[Double], max: Iterable[Double]) = this(min.toArray, max.toArray);

    def this(points: Iterable[Double]) = this(points.toArray, points.toArray);

    def this(point: Vector) = this(point.toArray, point.toArray);

    def indices: Range = min.indices;

    def minimum: Seq[Double] = min;

    def maximum: Seq[Double] = max;

    def isEmpty: Boolean = min(0) > max(0);

    def setEmpty(): Unit = {
        indices.foreach(i => {
            min(i) = Double.MaxValue;
            max(i) = Double.MinValue;
        });
    }

    def dimension: Int = min.length;

    def add(point: Seq[Double]): EnvelopeDoubleBuffer = {
        assert(dimension == point.length);
        indices.foreach(i => {
            if (min(i) > max(i)) { // Empty
                min(i) = point(i);
                max(i) = point(i);
            } else {
                if (point(i) < min(i)) {
                    min(i) = point(i);
                }
                if (point(i) > max(i)) {
                    max(i) = point(i);
                }
            }
        });
        this;
    }

    def add(point: Vector): EnvelopeDoubleBuffer = {
        assert(dimension == point.size);
        indices.foreach(i => {
            if (min(i) > max(i)) { // Empty
                min(i) = point(i);
                max(i) = point(i);
            } else {
                if (point(i) < min(i)) {
                    min(i) = point(i);
                }
                if (point(i) > max(i)) {
                    max(i) = point(i);
                }
            }
        });
        this;
    }

    def add(envelope: EnvelopeDoubleBuffer): EnvelopeDoubleBuffer = {
        assert(dimension == envelope.dimension);
        indices.foreach(i => {
            if (min(i) > max(i)) { // Empty
                min(i) = envelope.min(i);
                max(i) = envelope.max(i);
            } else {
                if (envelope.min(i) < min(i)) {
                    min(i) = envelope.min(i);
                }
                if (envelope.max(i) > max(i)) {
                    max(i) = envelope.max(i);
                }
            }
        });
        this;
    }

    override def hashCode: Int = Arrays.hashCode(min) ^ Arrays.hashCode(max)

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: EnvelopeDoubleBuffer => Arrays.equals(min, other.min) && Arrays.equals(max, other.max)
            case _ => false
        }
    }

    override def toString: String = s"[ ${Arrays.toString(min)} ; ${Arrays.toString(max)} ]";
}
