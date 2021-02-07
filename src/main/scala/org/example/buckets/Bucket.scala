package org.example.buckets

import org.apache.spark.mllib.linalg.Vector
import org.example.{EnvelopeDoubleBuffer, HashOptions}

import scala.collection.Iterable
import scala.collection.mutable.ArrayBuffer

class Bucket(val envelope: EnvelopeDoubleBuffer,
             val points: ArrayBuffer[Vector] = ArrayBuffer())
    extends Iterable[Vector] with Serializable {

    def this(dim: Int) = this(new EnvelopeDoubleBuffer(dim));

    def this(options: HashOptions) = this(dim = options.dim);

    def this(point: Vector) = {
        this(dim = point.size);
        put(point);
    }

    def put(pt: Vector): Unit = {
        points += pt;
        envelope.add(pt);
    }

    override def iterator: Iterator[Vector] = points.iterator;
}
