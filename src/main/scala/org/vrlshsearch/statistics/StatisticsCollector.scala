package org.vrlshsearch.statistics

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}

import java.nio.charset.StandardCharsets
import java.nio.file.Path

trait StatisticsCollector {
    def collect(row: StatisticsCollector.Row)

    def csv(file: Path)
}

object StatisticsCollector {

    trait Row {
        def headers(): Seq[String]

        def data(): Seq[Any]

        var dataset: String
        var t: Int
        var k: Int
    }
}

class DefaultStatisticsCollector(var statistics: List[StatisticsCollector.Row] = List()) extends StatisticsCollector {
    def collect(row: StatisticsCollector.Row): Unit = statistics = statistics :+ row

    def csv(file: Path): Unit = {
        if (statistics.nonEmpty) {
            val first = statistics.head
            val headers: Array[String] = Array[String]("dataset", "t", "k") ++ first.headers().toArray.toArray[String]

            val settings = new CsvWriterSettings
            settings.setHeaders(headers: _*)
            settings.setHeaderWritingEnabled(true)
            settings.getFormat.setDelimiter(",")
            settings.getFormat.setLineSeparator("\n")

            val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)
            statistics.foreach(row => {
                val data: Array[AnyRef] = Array[AnyRef](row.dataset, Int.box(row.t), Int.box(row.k)) ++ row.data().map(_.asInstanceOf[AnyRef]).toArray[AnyRef]
                csvWriter.writeRow(data: _*)
            })
            csvWriter.close()
        }
    }
}
