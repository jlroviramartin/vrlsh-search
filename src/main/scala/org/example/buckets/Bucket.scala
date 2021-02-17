package org.example.buckets

import scala.collection.mutable.TreeMap
import org.apache.spark.ml.linalg.Vector

import scala.collection.Iterable
import scala.collection.mutable.ArrayBuffer
import org.example.{Distance, EnvelopeDoubleBuffer, HashOptions}

class Bucket(val envelope: EnvelopeDoubleBuffer,
             val points: ArrayBuffer[Vector] = ArrayBuffer())
    extends Iterable[Vector] with Serializable {

    def this(dimension: Int) = this(new EnvelopeDoubleBuffer(dimension));

    def this(options: HashOptions) = this(dimension = options.dim);

    def this(point: Vector) = {
        this(dimension = point.size);
        put(point);
    }

    def put(point: Vector): Unit = {
        points += point;
        envelope.add(point);
    }

    override def iterator: Iterator[Vector] = points.iterator;

    def nearest(collector: KNearestNeighbors): Unit = points.foreach(collector.put(_))
}
