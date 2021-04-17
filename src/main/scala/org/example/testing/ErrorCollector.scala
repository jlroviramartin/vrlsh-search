package org.example.testing

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.example.Errors.{localDistanceError, localIndexError}

import java.nio.charset.StandardCharsets
import java.nio.file.Path

/**
 * Clase que recoge los errores.
 */
trait ErrorCollector {
    // (id: Long, distance: Double, realDistance: Double, index: Int, realIndex: Int)
    def collect(data: Iterable[(Long, Double, Double, Long, Long)], recall: Double): Unit

    def csv(file: Path): Unit
}

class DefaultErrorCollector(val count: Long,
                            val maxDistance: Double,
                            val k: Int) extends ErrorCollector {

    /*
        val count = data.count()
        val maxDistance = envelope.maxDistance
     */

    // List[(avgIndexError: Double, avgDistanceError: Double, size: Int)]
    private var errors: List[(Double, Double, Int, Double)] = List()

    /**
     * Obtiene los errores de los k puntos mas cercanos.
     *
     * @param data
     */
    // (id: Long, distance: Double, realDistance: Double, index: Int, realIndex: Int)
    def collect(data: Iterable[(Long, Double, Double, Long, Long)], recall: Double): Unit = {

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

                (id, indexError, distanceError)
            }
            .toArray

        // Se ajusta errors para que tenga siempre k elementos: si tiene menos, se toman como error máximo
        // Iterador de tamaño k con errores máximos (_, 1, 1)
        val maxErrors = (1 to k).map(_ => (-1, 1.0, 1.0))
        val adjustedErrors = (errors ++ maxErrors).take(k)

        // Se calcula la suma de todos los errores...
        val (sumIndexError: Double, sumDistanceError: Double, size: Int) = adjustedErrors
            .map { case (_, indexError, distanceError) => (indexError, distanceError) }
            .foldLeft((0.0, 0.0, 0)) {
                case ((accIndexError, accDistanceError, accCount), (indexError, distanceError)) =>
                    (accIndexError + indexError, accDistanceError + distanceError, accCount + 1)
            }

        // Se calcula la media
        val (avgIndexError, avgDistanceError) = (sumIndexError / size, sumDistanceError / size)

        //val size = errors.size
        //val avgIndexError = (errors.map { case (indexError, _) => indexError }.sum) / size
        //val avgDistanceError = (errors.map { case (_, distanceError) => distanceError }.sum) / size

        //println(f"Index error avg.: $avgIndexError%1.5f")
        //println()

        //println(f"Distance error avg.: $avgDistanceError%1.5f")
        //println()

        this.errors = this.errors :+ (avgIndexError, avgDistanceError, errors.size /*size*/ , recall)
    }

    def showAllErrors(): Unit = {
        println("Index error avg. |  Distance error avg. | Size | Recall")
        errors.foreach { case (avgIndexError, avgDistanceError, size, recall) => {
            println(f"$avgIndexError%1.5f | $avgDistanceError%1.5f | $size | $recall")
        }
        }
        println()
    }

    def showAverageOfErrors(): Unit = {
        // Se calcula la suma de todos los errores...
        val (sumIndexError: Double, sumDistanceError: Double, sumRecall: Double, sumSize: Double, count: Int) = errors
            .foldLeft((0.0, 0.0, 0.0, 0.0, 0)) {
                case ((accIndexError, accDistanceError, accRecall, accSize, accCount), (indexError, distanceError, size, recall)) =>
                    (accIndexError + indexError, accDistanceError + distanceError, accRecall + recall, accSize + size, accCount + 1)
            }

        // Se calcula la media
        val (avgIndexError, avgDistanceError, avgSize, avgRecall) = (sumIndexError / count, sumDistanceError / count, sumSize / count, sumRecall / count)

        //val size = errors.length
        //val avgIndexError = (errors.map { case (indexError, _, _) => indexError }.sum) / size
        //val avgDistanceError = (errors.map { case (_, distanceError, _) => distanceError }.sum) / size

        val numberOfSemiFailures = errors.count { case (_, _, length, _) => length > 0 && length < k }
        val numberOfFailures = errors.count { case (_, _, length, _) => length == 0 }

        println(f"Index error avg.: $avgIndexError%1.5f")
        println()

        println(f"Distance error avg.: $avgDistanceError%1.5f")
        println()

        println(f"Recall avg.: $avgRecall%1.5f")
        println()

        println(f"Size avg.: $avgSize%1.5f")
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

    def csv(file: Path) = {
        if (errors.nonEmpty) {
            val headers = Array("avgIndexError", "avgDistanceError", "size", "recall")

            val settings = new CsvWriterSettings
            settings.setHeaders(headers: _*)
            settings.setHeaderWritingEnabled(true)
            settings.getFormat().setDelimiter(",")
            settings.getFormat().setLineSeparator("\n")

            val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)
            errors.foreach(row => {
                val data = row.productIterator.map(_.asInstanceOf[AnyRef]).toArray
                csvWriter.writeRow(data: _*)
            })
            csvWriter.close()
        }
    }

}
