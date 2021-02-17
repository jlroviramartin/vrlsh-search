package org.example.buckets

import org.apache.spark.ml.linalg.Vector
import org.example.Distance

import scala.collection.Iterable
import scala.collection.mutable.TreeMap

class KNearestNeighbors(private val points: TreeMap[Double, Vector] = new TreeMap(),
                        val queryPoint: Vector,
                        val k: Int)
    extends Iterable[Vector] with Serializable {

    def put(point: Vector): Unit = {
        val d = Distance.squareDistance(queryPoint, point)
        put(d, point)
    }

    def put(other: KNearestNeighbors): Unit = {
        // NOTA: se puede optimizar usando los límites
        other.points.foreach { case (d, point) => put(d, point) };
    }

    private def put(d: Double, point: Vector): Unit = {
        if (points.size < k) {
            points.put(d, point);
        } else if (d < max) {
            points.remove(points.lastKey)
            points.put(d, point);
        }
    }

    def min: Double = if (points.size == 0) Double.MaxValue else points.firstKey;

    def max: Double = if (points.size == 0) Double.MinValue else points.lastKey;

    override def iterator: Iterator[Vector] = points.values.iterator;
}
