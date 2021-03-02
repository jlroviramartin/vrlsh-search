package org.example.buckets

import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable
import org.example.evaluators.{EuclideanHashEvaluator, Hash, TransformHashEvaluator}
import org.example.HashOptions

class TrBuckets(hashEvaluator: TransformHashEvaluator)
    extends Buckets(
        hashEvaluator,
        new mutable.HashMap[Double, mutable.Map[Hash, Bucket]]()) {

    def this(options: HashOptions) = {
        this(new EuclideanHashEvaluator(options));
    }

    override def put(point: Vector, resolution: Double): Unit = {
        val transformed = hashEvaluator.transform(point);
        val hash = hashEvaluator.hashTransformed(transformed, resolution);

        map.get(resolution) match {
            case Some(innerMap) => innerMap.get(hash) match {
                case Some(bucket: TrBucket) => bucket.put(Vectors.dense(transformed.toArray), point);
                case Some(bucket: Bucket) => bucket.put(point);
                case None => {
                    Console.println("Creating bucket " + hash);
                    innerMap.put(hash, new TrBucket(Vectors.dense(transformed.toArray), point));
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
