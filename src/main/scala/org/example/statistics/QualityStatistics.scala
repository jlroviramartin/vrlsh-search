package org.example.statistics

class QualityStatistics extends Serializable {
    var dataset: String = ""
    var t: Int = 0
    var k: Int = 0

    def desiredSize: Int = t * k

    var avgIndexError: Double = 0
    var avgDistanceErrorNorm: Double = 0
    var avgDistanceError: Double = 0
    var avgPrecision: Double = 0
    var recall: Double = 0
    var apk: Double = 0
}
