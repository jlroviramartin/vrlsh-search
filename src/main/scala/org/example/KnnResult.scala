package org.example

import scala.collection.immutable.{Iterable, TreeMap, TreeSet}

class KnnResult(val sorted: TreeSet[(Double, Long)]) extends Serializable {

    def this() = this(TreeSet())

    def size: Int = sorted.size
}

object KnnResult {

    def seqOp(k: Int)(accumulator: KnnResult, data: (Double, Long)): KnnResult = {
        val sorted = accumulator.sorted + data
        new KnnResult(sorted.take(k))
    }

    def seqOpOfList(k: Int)(accumulator: KnnResult, data: List[(Double, Long)]): KnnResult = {
        val sorted = accumulator.sorted ++ data
        new KnnResult(sorted.take(k))
    }

    def combOp(k: Int)(accumulator1: KnnResult, accumulator2: KnnResult): KnnResult = {
        val sorted = accumulator1.sorted ++ accumulator2.sorted
        new KnnResult(sorted.take(k))
    }
}
