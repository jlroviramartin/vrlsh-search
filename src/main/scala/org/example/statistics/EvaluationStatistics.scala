package org.example.statistics

class EvaluationStatistics(val size: Int,
                           val comparisons: Int,
                           val buckets: Int,
                           val numLevels: Int,
                           val radiuses: List[Double]) extends StatisticsCollector.Row with Serializable {

    def headers(): Seq[String] = List("size", "comparisons", "buckets", "numLevels")

    def data(): Seq[Any] = List(size, comparisons, buckets, numLevels)

    override def toString: String =
        s"size $size comparisons $comparisons buckets $buckets numLevels $numLevels radiuses $radiuses"
}