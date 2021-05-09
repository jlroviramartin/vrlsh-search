package org.example.buckets

import org.apache.spark.ml.linalg.Vector
import org.example.Distance

import scala.collection.{Iterable, mutable}

class KNearestNeighbors(private val points: mutable.TreeMap[Double, Vector] = new mutable.TreeMap(),
                        val queryPoint: Vector,
                        val k: Int)
    extends Iterable[Vector] with Serializable {

    def put(point: Vector): Unit = {
        val d = Distance.squareDistance(queryPoint, point)
        put(d, point)
    }

    def put(other: KNearestNeighbors): Unit = {
        // NOTA: se puede optimizar usando los límites
        other.points.foreach { case (d, point) => put(d, point) }
    }

    private def put(d: Double, point: Vector): Unit = {
        if (points.size < k) {
            points.put(d, point)
        } else if (d < max) {
            points.remove(points.lastKey)
            points.put(d, point)
        }
    }

    def min: Double = if (points.isEmpty) Double.MaxValue else points.firstKey

    def max: Double = if (points.isEmpty) Double.MinValue else points.lastKey

    override def iterator: Iterator[Vector] = points.values.iterator
}
