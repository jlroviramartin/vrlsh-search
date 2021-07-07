package org.vrlshsearch

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.vrlshsearch.construction.VrlshKnnConstructionAlgorithm
import org.vrlshsearch.statistics.{DefaultStatisticsCollector, GeneralStatistics, QualityStatistics}
import org.vrlshsearch.testing.TestingUtils
import org.vrlshsearch.testing.TestingUtils.doQueriesWithResult

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import scala.collection.immutable

object CsvUtils {

    def storeApproximateResult(outputDirectory: Path,
                               result: Iterable[(Long, Array[Long])],
                               k: Int): Unit = {

        if (result.nonEmpty) {
            val header = Array("id") ++ (0 until k).map(i => "k" + i)

            val settings = new CsvWriterSettings
            settings.setHeaders(header: _*)
            settings.setHeaderWritingEnabled(true)
            settings.getFormat.setDelimiter(",")
            settings.getFormat.setLineSeparator("\n")

            val csvWriter = new CsvWriter(outputDirectory.resolve("result.csv").toFile, StandardCharsets.UTF_8, settings)

            var index = 0
            result.foreach { case (id, approx) => {
                if (index % 100 == 0) {
                    //println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                val row = Array(Long.box(id)) ++
                    (0 until approx.length).map(i => Long.box(approx(i))) ++
                    (approx.length until k).map(i => Long.box(-1))

                csvWriter.writeRow(row: _*)
                //csvWriter.writeRow(id, approx)
            }
            }
            csvWriter.close()
        }
    }

    def storeGroundTruth(outputDirectory: Path,
                         result: Iterator[(Long, Array[Long])],
                         k: Int): Unit = {
        println("==== Writing csv =====")

        val file = outputDirectory.resolve(s"groundtruth.csv")

        val header = Array("id") ++ (0 until k).map(i => "k" + i)

        val settings = new CsvWriterSettings
        settings.setHeaders(header: _*)
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0
        result
            .foreach { case (queryId, array) => {
                if (index % 100 == 0) {
                    //println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                val row = Array(Long.box(queryId)) ++ (0 until k).map(i => Long.box(array(i)))
                csvWriter.writeRow(row: _*)
            }
            }

        csvWriter.close()
    }

    def storeMaxDistances(outputDirectory: Path,
                          result: Iterator[(Long, Double)]): Unit = {

        println("==== Writing csv =====")

        val file = outputDirectory.resolve("max-distances.csv")

        val settings = new CsvWriterSettings
        settings.setHeaders("id", "maxDistance")
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0
        result
            .foreach { case (id, maxDistance) => {
                if (index % 100 == 0) {
                    //println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                csvWriter.writeRow(Long.box(id), Double.box(maxDistance))
            }
            }

        csvWriter.close()
    }

    def storeGlobal(outputDirectory: Path,
                    data: Array[GeneralStatistics]): Unit = {

        println("==== Writing csv =====")

        val file = outputDirectory.resolve("global-statistics.csv")

        val settings = new CsvWriterSettings
        settings.setHeaders("dataset", "t", "k", "desiredSize", "numTables", "keyLength", "dimension", "ratioOfPoints", "totalNumBuckets", "totalNumPoints", "totalNumLevels")
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0
        data
            .foreach(statistics => {
                if (index % 100 == 0) {
                    //println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                csvWriter.writeRow(
                    statistics.dataset,
                    Int.box(statistics.t),
                    Int.box(statistics.k),
                    Int.box(statistics.desiredSize),
                    Int.box(statistics.numTables),
                    Int.box(statistics.keyLength),
                    Int.box(statistics.dimension),
                    Double.box(statistics.ratioOfPoints),
                    Int.box(statistics.totalNumBuckets),
                    Int.box(statistics.totalNumPoints),
                    Int.box(statistics.totalNumLevels))
            })

        csvWriter.close()
    }

    def storeQuality(outputDirectory: Path,
                     data: Array[QualityStatistics]): Unit = {

        println("==== Writing csv =====")

        val file = outputDirectory.resolve("quality-statistics.csv")

        val settings = new CsvWriterSettings
        settings.setHeaders(
            "dataset",
            "t",
            "k",
            "desiredSize",
            "avgIndexError",
            "avgDistanceErrorNorm",
            "avgDistanceError",
            "approxRatio",
            "recall",
            "avgPrecision")
        settings.setHeaderWritingEnabled(true)
        settings.getFormat.setDelimiter(",")
        settings.getFormat.setLineSeparator("\n")

        val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)

        var index = 0
        data
            .foreach(statistics => {
                if (index % 100 == 0) {
                    //println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                csvWriter.writeRow(
                    statistics.dataset,
                    Int.box(statistics.t),
                    Int.box(statistics.k),
                    Int.box(statistics.desiredSize),
                    Double.box(statistics.avgIndexError),
                    Double.box(statistics.avgDistanceErrorNorm),
                    Double.box(statistics.avgDistanceError),
                    Double.box(statistics.approxRatio),
                    Double.box(statistics.recall),
                    Double.box(statistics.avgPrecision)
                )
            })

        csvWriter.close()
    }

    def storeTimeResult(outputDirectory: Path,
                        result: Iterable[(String, Int, Int, Int, Long, Double)]): Unit = {

        if (result.nonEmpty) {
            val header = Array("dataset", "t", "k", "desiredSize", "constructionTime", "queryTime")

            val settings = new CsvWriterSettings
            settings.setHeaders(header: _*)
            settings.setHeaderWritingEnabled(true)
            settings.getFormat.setDelimiter(",")
            settings.getFormat.setLineSeparator("\n")

            val csvWriter = new CsvWriter(outputDirectory.resolve("time.csv").toFile, StandardCharsets.UTF_8, settings)

            var index = 0

            result.foreach { case (name, t, k, desiredSize, constructionTime, queryTime) => {
                if (index % 100 == 0) {
                    //println(s"> Procesando $index")
                    csvWriter.flush()
                }
                index = index + 1

                val row = Array(name,
                    Int.box(t),
                    Int.box(k), Int.box(desiredSize),
                    Long.box(constructionTime),
                    Double.box(queryTime))
                csvWriter.writeRow(row: _*)
            }
            }
            csvWriter.close()
        }
    }
}
