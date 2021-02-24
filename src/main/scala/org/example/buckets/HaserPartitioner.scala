package org.example.buckets

import org.apache.spark.Partitioner

class HaserPartitioner extends Partitioner {
    override def numPartitions: Int = ???

    override def getPartition(key: Any): Int = ???
}
