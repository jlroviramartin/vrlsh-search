package org.vrlshsearch

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

trait LookupProvider extends Serializable {
    def lookup(index: Long): Vector;
}

class BroadcastLookupProvider(bdata: Broadcast[scala.collection.Map[Long, Vector]]) extends LookupProvider {

    def this(data: RDD[(Long, Vector)]) = {
        //this(data.sparkContext.broadcast(data.sortBy(_._1).map(_._2).collect()))
        this(data.sparkContext.broadcast(data.collectAsMap()))
    }

    def lookup(index: Long): Vector = bdata.value(index.toInt)

    def size: Int = bdata.value.size
}
