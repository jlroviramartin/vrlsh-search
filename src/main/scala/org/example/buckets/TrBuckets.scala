package org.example.buckets

import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable

import org.example.evaluators.{HashPoint, TransformHashEvaluator, EuclideanHashEvaluator}
import org.example.HashOptions

class TrBuckets(hashEvaluator: TransformHashEvaluator)
    extends Buckets(
        hashEvaluator,
        new mutable.HashMap[Double, mutable.Map[HashPoint, Bucket]]()) {

    def this(options: HashOptions) = {
        this(new EuclideanHashEvaluator(options));
    }

    override def put(point: Vector, resolution: Double): Unit = {
        val trPoint = hashEvaluator.transform(point);
        val hash = hashEvaluator.hashTransformed(trPoint, resolution);

        map.get(resolution) match {
            case Some(innerMap) => innerMap.get(hash) match {
                case Some(bucket: TrBucket) => bucket.put(trPoint, point);
                case Some(bucket: Bucket) => bucket.put(point);
                case None => {
                    Console.println("Creating bucket " + hash);
                    innerMap.put(hash, new TrBucket(trPoint, point));
                }
            }
            case None => {
                Console.println("Creating resolution " + resolution);
                map.put(resolution, new mutable.HashMap());
            }
        }
        //Console.println("MIERDA " + map);
        //Console.println("MIERDA " + getBuckets(resolution).size);
    }
}
