package org.example.testing

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.example.Errors

import java.nio.charset.StandardCharsets
import java.nio.file.Path

/**
 * Clase que recoge los errores.
 */
trait ErrorCollector {
    // (id: Long, distance: Double, realDistance: Double, index: Int, realIndex: Int)
    def collect(data: Iterable[(Long, Double, Double, Double, Long, Long)], recall: Double, apk: Double): Unit

    def csv(file: Path): Unit
}

class DefaultErrorCollector(val count: Long,
                            /*val maxDistance: Double,*/
                            val k: Int) extends ErrorCollector {

    /*
        val count = data.count()
        val maxDistance = envelope.maxDistance
     */

    // List[(avgIndexError: Double, avgDistanceError: Double, avgDistanceErrorNorm: Double, size: Int, recall: Double, apk: Double)]
    private var errors: List[(Double, Double, Double, Int, Double, Double)] = List()

    /**
     * Obtiene los errores de los k puntos mas cercanos.
     *
     * @param data
     */
    // (id: Long, distance: Double, realDistance: Double, index: Int, realIndex: Int)
    def collect(data: Iterable[(Long, Double, Double, Double, Long, Long)], recall: Double, apk: Double): Unit = {

        // Se muestra el resultado como una tabla md
        if (false) {
            println("| distance | id | index | realIndex | index error | distance error | distance error norm. |")
            println("| -: | -: | -: | -: | -: | -: | -: |")
            data
                .foreach { case (id, distance, realDistance, maxDistance, index, realIndex) =>
                    val indexError = Errors.localIndexError(index, realIndex, count)
                    val distanceError = Errors.localDistanceError(distance, realDistance, maxDistance /*- realDistance*/)
                    val distanceErrorNorm = Errors.distanceError(distance, realDistance)

                    println(f"| $distance | $id | $index | $realIndex | $indexError%1.5f | $distanceError%1.5f | $distanceErrorNorm%1.5f |")
                }
        }

        // Se calculan los errores para cada punto
        val errors = data
            .map { case (id, distance, realDistance, maxDistance, index, realIndex) =>
                val indexError = Errors.localIndexError(index, realIndex, count)
                val distanceError = Errors.localDistanceError(distance, realDistance, maxDistance)
                val distanceErrorNorm = Errors.distanceError(distance, realDistance)

                (id, indexError, distanceError, distanceErrorNorm)
            }
            .toArray

        // Se ajusta errors para que tenga siempre k elementos: si tiene menos, se toman como error máximo
        // Iterador de tamaño k con errores máximos (_, 1, 1)
        val maxErrors = (1 to k).map(_ => (-1, 1.0, 1.0))
        val adjustedErrors = (errors ++ maxErrors).take(k)

        // Se calcula la suma de todos los errores...
        val (sumIndexError: Double, sumDistanceError: Double, sumDistanceErrorNorm: Double, size: Int) = adjustedErrors
            .map { case (_, indexError, distanceError, distanceErrorNorm) => (indexError, distanceError, distanceErrorNorm) }
            .foldLeft((0.0, 0.0, 0.0, 0)) {
                case ((accIndexError, accDistanceError, accDistanceErrorNorm, accCount), (indexError: Double, distanceError: Double, distanceErrorNorm: Double)) =>
                    (accIndexError + indexError, accDistanceError + distanceError, accDistanceErrorNorm + distanceErrorNorm, accCount + 1)
            }

        // Se calcula la media
        val (avgIndexError, avgDistanceError, avgDistanceErrorNorm) = (sumIndexError / size, sumDistanceError / size, sumDistanceErrorNorm / size)

        //val size = errors.size
        //val avgIndexError = (errors.map { case (indexError, _) => indexError }.sum) / size
        //val avgDistanceError = (errors.map { case (_, distanceError) => distanceError }.sum) / size

        //println(f"Index error avg.: $avgIndexError%1.5f")

        //println(f"Distance error avg.: $avgDistanceError%1.5f")

        this.errors = this.errors :+ (avgIndexError, avgDistanceError, avgDistanceErrorNorm, errors.size, recall, apk)
    }

    def showAllErrors(): Unit = {
        println("Index error avg. |  Distance error avg. | Size | Recall")
        errors.foreach { case (avgIndexError, avgDistanceError, avgDistanceErrorNorm, size, recall, apk) => {
            println(f"$avgIndexError%1.5f | $avgDistanceError%1.5f | $avgDistanceErrorNorm%1.5f | $size | $recall | $apk")
        }
        }
    }

    def showAverageOfErrors(): Unit = {
        // Se calcula la suma de todos los errores...
        val (sumIndexError: Double, sumDistanceError: Double, sumDistanceErrorNorm: Double, sumRecall: Double, sumApk: Double, sumSize: Double, count: Int) = errors
            .foldLeft((0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)) {
                case (
                    (accIndexError, accDistanceError, accDistanceErrorNorm, accRecall, accApk, accSize, accCount),
                    (indexError: Double, distanceError: Double, distanceErrorNorm: Double, size: Int, recall: Double, apk: Double)) =>

                    (accIndexError + indexError, accDistanceError + distanceError, accDistanceErrorNorm + distanceErrorNorm, accRecall + recall, accApk + apk, accSize + size, accCount + 1)
            }

        // Se calcula la media
        val (avgIndexError, avgDistanceError, avgDistanceErrorNorm, avgSize, avgRecall, avgApk) = (sumIndexError / count, sumDistanceError / count, sumDistanceErrorNorm / count, sumSize / count, sumRecall / count, sumApk / count)

        //val size = errors.length
        //val avgIndexError = (errors.map { case (indexError, _, _) => indexError }.sum) / size
        //val avgDistanceError = (errors.map { case (_, distanceError, _) => distanceError }.sum) / size

        val numberOfSemiFailures = errors.count { case (_, _, _, _, length, _) => length > 0 && length < k }
        val numberOfFailures = errors.count { case (_, _, _, _, length, _) => length == 0 }

        println(f"Index error avg.: $avgIndexError%1.5f")

        println(f"Distance error avg.: $avgDistanceError%1.5f")

        println(f"Distance error norm. avg.: $avgDistanceErrorNorm%1.5f")

        println(f"Recall avg.: $avgRecall%1.5f")

        println(f"Apk avg.: $avgApk%1.5f")

        println(f"Size avg.: $avgSize%1.5f")

        if (numberOfSemiFailures > 0) {
            println(s"No existe solución completa: $numberOfSemiFailures")
        }

        if (numberOfFailures > 0) {
            println(s"No existe NINGUNA solución: $numberOfFailures")
        }
    }

    def csv(file: Path) = {
        if (errors.nonEmpty) {
            val headers = Array("avgIndexError", "avgDistanceError", "avgDistanceErrorNorm", "size", "recall", "apk")

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
