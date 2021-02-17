package org.example

import org.apache.spark.ml.linalg.Vector

class EnvelopeDoubleBuffer(val min: Array[Double], val max: Array[Double]) {
    def this(dimension: Int) = {
        this(new Array[Double](dimension), new Array[Double](dimension));
        setEmpty();
    }

    def this(min: Seq[Double], max: Seq[Double]) = this(min.toArray, max.toArray);

    def this(point: Seq[Double]) = this(point.toArray, point.toArray);

    def this(point: Vector) = this(point.toArray, point.toArray);

    def indices: Range = min.indices;

    def minimum: Seq[Double] = min;

    def maximum: Seq[Double] = max;

    def isEmpty: Boolean = {
        min(0) > max(0);
    }

    def setEmpty(): Unit = {
        indices.foreach(i => {
            min(i) = Double.MaxValue;
            max(i) = Double.MinValue;
        });
    }

    def dimension: Int = min.length;

    def add(point: Seq[Double]): Unit = {
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
    }

    def add(point: Vector): Unit = {
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
    }

    def add(envelope: EnvelopeDoubleBuffer): Unit = {
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
    }
}
