package org.vrlshsearch.buckets

import org.apache.spark.ml.linalg.Vector

import scala.collection.{Iterable, mutable}

import org.vrlshsearch.evaluators.{HashEvaluator, Hash}

class Buckets(val hashEvaluator: HashEvaluator,
              val map: mutable.Map[Double, mutable.Map[Hash, Bucket]])
    extends Serializable {

    def put(point: Vector, resolution: Double): Unit = {
        val hash = hashEvaluator.hash(point, resolution)

        map.get(resolution) match {
            case Some(map) => map.get(hash) match {
                case Some(bucket) => bucket.put(point);
                case None => map.put(hash, new Bucket(point));
            };
            case None => map.put(resolution, new mutable.HashMap());
        }
    }

    def getBuckets(resolution: Double): Iterable[(Hash, Bucket)] = {
        map.get(resolution) match {
            case Some(map) => map
            case None => Iterable.empty
        }
    }
}
