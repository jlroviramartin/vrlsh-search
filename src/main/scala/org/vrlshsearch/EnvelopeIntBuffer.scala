package org.vrlshsearch

import java.util.Arrays

class EnvelopeIntBuffer(val min: Array[Int], val max: Array[Int]) extends Serializable {

    def this(dimension: Int) = {
        this(new Array[Int](dimension), new Array[Int](dimension))
        setEmpty()
    }

    def this(min: Iterable[Int], max: Iterable[Int]) = this(min.toArray, max.toArray)

    def this(points: Iterable[Int]) = this(points.toArray, points.toArray)

    def indices: Range = min.indices

    def minimum: Seq[Int] = min

    def maximum: Seq[Int] = max

    def isEmpty: Boolean = min(0) > max(0)

    def setEmpty(): Unit = {
        indices.foreach(i => {
            min(i) = Int.MaxValue
            max(i) = Int.MinValue
        })
    }

    def dimension: Int = min.length

    def add(point: Seq[Int]): Unit = {
        assert(dimension == point.length)
        indices.foreach(i => {
            if (min(i) > max(i)) { // Empty
                min(i) = point(i)
                max(i) = point(i)
            } else {
                if (point(i) < min(i)) {
                    min(i) = point(i)
                }
                if (point(i) > max(i)) {
                    max(i) = point(i)
                }
            }
        })
    }

    def add(envelope: EnvelopeIntBuffer): Unit = {
        assert(dimension == envelope.dimension)
        indices.foreach(i => {
            if (min(i) > max(i)) { // Empty
                min(i) = envelope.min(i)
                max(i) = envelope.max(i)
            } else {
                if (envelope.min(i) < min(i)) {
                    min(i) = envelope.min(i)
                }
                if (envelope.max(i) > max(i)) {
                    max(i) = envelope.max(i)
                }
            }
        })
    }

    override def hashCode: Int = Arrays.hashCode(min) ^ Arrays.hashCode(max)

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: EnvelopeIntBuffer => Arrays.equals(min, other.min) && Arrays.equals(max, other.max)
            case _ => false
        }
    }

    override def toString: String = s"[ ${Arrays.toString(min)}  ${Arrays.toString(max)} ]"
}
