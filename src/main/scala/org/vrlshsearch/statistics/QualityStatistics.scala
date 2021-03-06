package org.vrlshsearch.statistics

class QualityStatistics extends Serializable {
    var dataset: String = ""
    var t: Int = 0
    var k: Int = 0

    def desiredSize: Int = t * k

    var avgIndexError: Double = 0
    var avgDistanceErrorNorm: Double = 0
    var avgDistanceError: Double = 0
    var approxRatio: Double = 0
    var recall: Double = 0
    var avgPrecision: Double = 0
}
