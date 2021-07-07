package org.vrlshsearch

import scala.collection.immutable.TreeSet

class KnnResult(val sorted: TreeSet[(Double, Long)],
                val comparisons: Int,
                val buckets: Int) extends Serializable {

    def this() = this(TreeSet(), 0, 0)

    def size: Int = sorted.size
}

object KnnResult {

    def seqOp(k: Int)(accumulator: KnnResult, data: (Double, Long)): KnnResult = {
        val sorted = accumulator.sorted + data

        new KnnResult(sorted.take(k),
            accumulator.comparisons + 1,
            accumulator.buckets + 1)
    }

    def seqOpOfList(k: Int)(accumulator: KnnResult, bucket: Seq[(Double, Long)]): KnnResult = {
        val sorted = accumulator.sorted ++ bucket

        new KnnResult(sorted.take(k),
            accumulator.comparisons + bucket.length,
            accumulator.buckets + 1)
    }

    def seqOpOfArray(k: Int)(accumulator: KnnResult, bucket: Array[(Double, Long)]): KnnResult = {
        val sorted = accumulator.sorted ++ bucket

        new KnnResult(sorted.take(k),
            accumulator.comparisons + bucket.length,
            accumulator.buckets + 1)
    }

    def combOp(k: Int)(accumulator1: KnnResult, accumulator2: KnnResult): KnnResult = {
        val sorted = accumulator1.sorted ++ accumulator2.sorted

        new KnnResult(sorted.take(k),
            accumulator1.comparisons + accumulator2.comparisons,
            accumulator1.buckets + accumulator2.buckets)
    }
}
