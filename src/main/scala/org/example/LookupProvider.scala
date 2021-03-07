package org.example

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

trait LookupProvider extends Serializable {
    def lookup(index: Long): Vector;
}

class BroadcastLookupProvider(bdata: Broadcast[Array[Vector]]) extends LookupProvider {

    def this(data: RDD[(Long, Vector)]) = {
        this(data.sparkContext.broadcast(data.sortBy(_._1).map(_._2).collect()))
    }

    def lookup(index: Long): Vector = bdata.value(index.toInt)
}
