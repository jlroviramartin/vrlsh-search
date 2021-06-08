package org.example.statistics

class GeneralStatistics extends Serializable {
    var dataset: String = ""
    var t: Int = 0
    var k: Int = 0

    def desiredSize: Int = t * k

    var numTables: Int = 0
    var keyLength: Int = 0
    var dimension: Int = 0

    var ratioOfPoints: Double = 0
    var totalNumBuckets: Int = 0
    var totalNumPoints: Int = 0
    var totalNumLevels: Int = 0
}
