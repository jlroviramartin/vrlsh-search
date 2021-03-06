package org.vrlshsearch.buckets

import org.apache.spark.ml.linalg.Vector
import org.vrlshsearch.evaluators.{Hash, HashPoint}

import scala.collection.Iterable
import scala.collection.mutable.ArrayBuffer
import org.vrlshsearch.{DataStore, EnvelopeDoubleBuffer, HashOptions}

import java.nio.file.{Files, Path}

class Bucket(val envelope: EnvelopeDoubleBuffer,
             val points: ArrayBuffer[Vector] = ArrayBuffer())
    extends Iterable[Vector] with Serializable {

    def this(dimension: Int) = this(new EnvelopeDoubleBuffer(dimension))

    def this(options: HashOptions) = this(dimension = options.dim)

    def this(point: Vector) = {
        this(dimension = point.size)
        put(point)
    }

    def this(it: Iterable[Vector]) = {
        this(it.head)
        it.tail.foreach(v => put(v))
    }

    def put(point: Vector): Unit = {
        points += point
        envelope.add(point)
    }

    override def iterator: Iterator[Vector] = points.iterator;

    def nearest(collector: KNearestNeighbors): Unit = points.foreach(collector.put)

    override def toString(): String = {
        val buff = new StringBuilder
        points.foreach(buff.append(_).append("%n"))
        buff.toString()
    }

    def store(radius: Double, hash: Hash, baseDirectory: Path): Unit = {
        var realDirectory = baseDirectory

        realDirectory = realDirectory.resolve(radius.toString)
        HashPoint.get(hash).values.foreach(value => realDirectory = realDirectory.resolve(value.toString))
        Files.createDirectories(realDirectory)

        DataStore.store(realDirectory.resolve("bucket.data"), this)
    }
}

object Bucket {

    def load(radius: Double, hash: Hash, baseDirectory: Path): Option[Bucket] = {
        var realDirectory = baseDirectory

        realDirectory = realDirectory.resolve(radius.toString)
        HashPoint.get(hash).values.foreach(value => realDirectory = realDirectory.resolve(value.toString))
        if (!Files.exists(realDirectory.resolve("bucket.data"))) {
            return Option.empty
        }

        Option.apply(DataStore.load[Bucket](realDirectory.resolve("bucket.data")))
    }
}
