package org.example.construction

trait StatisticsCollector {
    def collect(row: StatisticsCollector.Row)
}

object StatisticsCollector {

    trait Row {
    }

}

class DefaultStatisticsCollector(var statistics: List[StatisticsCollector.Row] = List()) extends StatisticsCollector {
    def collect(row: StatisticsCollector.Row) = statistics = statistics :+ row
}
