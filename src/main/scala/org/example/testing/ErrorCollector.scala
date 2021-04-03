package org.example.testing

import org.example.Errors.{localDistanceError, localIndexError}


/**
 * Clase que recoge los errores.
 */
trait ErrorCollector {
    // (id: Long, distance: Double, realDistance: Double, index: Int, realIndex: Int)
    def collect(data: Iterable[(Long, Double, Double, Long, Long)]): Unit;
}

class DefaultErrorCollector(val count: Long,
                            val maxDistance: Double,
                            val k: Int) extends ErrorCollector {

    /*
        val count = data.count()
        val maxDistance = envelope.maxDistance
     */

    // List[(avgIndexError: Double, avgDistanceError: Double, size: Int)]
    private var errors: List[(Double, Double, Int)] = List()

    // (id: Long, distance: Double, realDistance: Double, index: Int, realIndex: Int)
    def collect(data: Iterable[(Long, Double, Double, Long, Long)]): Unit = {

        // Se muestra el resultado como una tabla md
        if (false) {
            println("| distance | id | index | realIndex | index error | distance error |")
            println("| -: | -: | -: | -: | -: | -: |")
            data
                .foreach { case (id, distance, realDistance, index, realIndex) =>
                    val indexError = localIndexError(index, realIndex, count)
                    val distanceError = localDistanceError(distance, realDistance, maxDistance)

                    println(f"| $distance | $id | $index | $realIndex | $indexError%1.5f | $distanceError%1.5f |")
                }
            println()
        }

        // Se calculan los errores para cada punto
        val errors = data
            .map { case (id, distance, realDistance, index, realIndex) =>
                val indexError = localIndexError(index, realIndex, count)
                val distanceError = localDistanceError(distance, realDistance, maxDistance)

                (indexError, distanceError, 1)
            }

        // Se reducen los errores...
        //val (sumIndexError: Double, sumDistanceError: Double, size: Int) = errors
        //    .reduce { case ((indexError1, distanceError1, count1), (indexError2, distanceError2, count2)) => (indexError1 + indexError2, distanceError1 + distanceError2, count1 + count2) }
        val size = errors.size
        val avgIndexError = (errors.map { case (indexError, _, _) => indexError }.sum) / size
        val avgDistanceError = (errors.map { case (_, distanceError, _) => distanceError }.sum) / size

        // Se calcula la media
        //val (avgIndexError, avgDistanceError) = (sumIndexError / size, sumDistanceError / size)

        println(f"Index error avg.: $avgIndexError%1.5f")
        println()

        println(f"Distance error avg.: $avgDistanceError%1.5f")
        println()

        this.errors = this.errors :+ (avgIndexError, avgDistanceError, size)
    }

    def showAverageOfErrors(k: Int): Unit = {
        val size = errors.length
        val avgIndexError = (errors.map { case (indexError, _, _) => indexError }.sum) / size
        val avgDistanceError = (errors.map { case (_, distanceError, _) => distanceError }.sum) / size

        val numberOfSemiFailures = errors.count { case (_, _, length) => length > 0 && length < k }
        val numberOfFailures = errors.count { case (_, _, length) => length == 0 }

        println(f"Index error avg.: $avgIndexError%1.5f")
        println()

        println(f"Distance error avg.: $avgDistanceError%1.5f")
        println()

        if (numberOfSemiFailures > 0) {
            println(s"No existe solución completa: $numberOfSemiFailures")
            println()
        }

        if (numberOfFailures > 0) {
            println(s"No existe NINGUNA solución: $numberOfFailures")
            println()
        }
    }
}
