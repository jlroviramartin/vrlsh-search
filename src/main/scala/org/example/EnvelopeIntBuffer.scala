package org.example

class EnvelopeIntBuffer(val min: Array[Int], val max: Array[Int]) {
    def this(dimension: Int) = {
        this(new Array[Int](dimension), new Array[Int](dimension));
        setEmpty();
    }

    def this(min: Seq[Int], max: Seq[Int]) = this(min.toArray, max.toArray);

    def this(pt: Seq[Int]) = this(pt.toArray, pt.toArray);

    def indices: Range = min.indices;

    def minimum: Seq[Int] = min;

    def maximum: Seq[Int] = max;

    def isEmpty: Boolean = {
        min(0) > max(0);
    }

    def setEmpty(): Unit = {
        indices.foreach(i => {
            min(i) = Int.MaxValue;
            max(i) = Int.MinValue;
        });
    }

    def dimension: Int = min.length;

    def add(point: Seq[Int]): Unit = {
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

    def add(envelope: EnvelopeIntBuffer): Unit = {
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
