package org.example

import scala.collection.immutable.TreeMap

class KnnResult(val sorted: TreeMap[Double, Long]) extends Serializable {

    def this() = this(TreeMap())
}

object KnnResult {

    def seqOp(k: Int)(accumulator: KnnResult, data: (Double, Long)): KnnResult = {
        val sorted = accumulator.sorted + data
        new KnnResult(sorted.take(k))
    }

    def combOp(k: Int)(accumulator1: KnnResult, accumulator2: KnnResult): KnnResult = {
        val sorted = accumulator1.sorted ++ accumulator2.sorted
        new KnnResult(sorted.take(k))
    }
}
