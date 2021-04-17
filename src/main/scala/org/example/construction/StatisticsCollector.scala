package org.example.construction

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
    }

}

class DefaultStatisticsCollector(var statistics: List[StatisticsCollector.Row] = List()) extends StatisticsCollector {
    def collect(row: StatisticsCollector.Row) = statistics = statistics :+ row

    def csv(file: Path) = {
        if (statistics.nonEmpty) {
            val first = statistics(0)
            val headers = first.headers().toArray

            val settings = new CsvWriterSettings
            settings.setHeaders(headers: _*)
            settings.setHeaderWritingEnabled(true)
            settings.getFormat().setDelimiter(",")
            settings.getFormat().setLineSeparator("\n")

            val csvWriter = new CsvWriter(file.toFile, StandardCharsets.UTF_8, settings)
            statistics.foreach(row => {
                val data = row.data().map(_.asInstanceOf[AnyRef]).toArray
                csvWriter.writeRow(data: _*)
            })
            csvWriter.close()
        }
    }
}
