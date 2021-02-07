package org.example.buckets

import org.apache.spark.mllib.linalg.Vector
import org.example.evaluators.{HashEvaluator, HashPoint}

import scala.collection.{Iterable, mutable}

class Buckets(val hashEvaluator: HashEvaluator,
              val map: mutable.Map[Double, mutable.Map[HashPoint, Bucket]])
    extends Serializable {

    def put(point: Vector, resolution: Double): Unit = {
        val hash = hashEvaluator.hash(point, resolution);

        map.get(resolution) match {
            case Some(map) => map.get(hash) match {
                case Some(bucket) => bucket.put(point);
                case None => map.put(hash, new Bucket(point));
            };
            case None => map.put(resolution, new mutable.HashMap());
        }
    }

    def getBuckets(resolution: Double): Iterable[(HashPoint, Bucket)] = {
        map.get(resolution) match {
            case Some(map) => {
                Console.println("MIERDA " + map.size);
                map
            }
            case None => Iterable.empty
        }
    }
}
