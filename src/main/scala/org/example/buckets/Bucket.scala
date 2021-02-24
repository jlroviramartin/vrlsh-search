package org.example.buckets

import scala.collection.mutable.TreeMap
import org.apache.spark.ml.linalg.Vector
import org.example.evaluators.HashPoint

import scala.collection.Iterable
import scala.collection.mutable.ArrayBuffer
import org.example.{DataStore, Distance, EnvelopeDoubleBuffer, HashOptions, Utils}

import java.nio.file.{Files, Path, Paths}

class Bucket(val envelope: EnvelopeDoubleBuffer,
             val points: ArrayBuffer[Vector] = ArrayBuffer())
    extends Iterable[Vector] with Serializable {

    def this(dimension: Int) = this(new EnvelopeDoubleBuffer(dimension));

    def this(options: HashOptions) = this(dimension = options.dim);

    def this(point: Vector) = {
        this(dimension = point.size);
        put(point);
    }

    def this(it: Iterable[Vector]) = {
        this(it.head);
        it.tail.foreach(v => put(v))
    }

    def put(point: Vector): Unit = {
        points += point;
        envelope.add(point);
    }

    override def iterator: Iterator[Vector] = points.iterator;

    def nearest(collector: KNearestNeighbors): Unit = points.foreach(collector.put(_))

    override def toString(): String = {
        val buff = new StringBuilder;
        points.foreach(buff.append(_).append("%n"))
        buff.toString()
    }

    def store(radius: Double, hash: HashPoint, baseDirectory: Path): Unit = {
        var realDirectory = baseDirectory

        realDirectory = realDirectory.resolve(radius.toString)
        hash.values.foreach(value => realDirectory = realDirectory.resolve(value.toString))
        Files.createDirectories(realDirectory)

        DataStore.store(realDirectory.resolve("bucket.data"), this)
    }
}

object Bucket {

    def load(radius: Double,hash: HashPoint, baseDirectory: Path): Option[Bucket] = {
        var realDirectory = baseDirectory

        realDirectory = realDirectory.resolve(radius.toString)
        hash.values.foreach(value => realDirectory = realDirectory.resolve(value.toString))
        if (!Files.exists(realDirectory.resolve("bucket.data"))) {
            return Option.empty
        }

        Option.apply(DataStore.load[Bucket](realDirectory.resolve("bucket.data")))
    }
}
