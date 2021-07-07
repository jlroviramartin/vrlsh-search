package org.vrlshsearch

import scala.collection.Set

class Aggregator[T](var count: Int = 0, var values: Set[T] = Set[T]()) extends Serializable

object Aggregator {
    def zero[T] = new Aggregator[T](0, Set[T]())

    def add[T](agg: Aggregator[T], values: Traversable[T]) = {
        for (c <- values)
            agg.values += c
        agg
    }

    def addSample[T](agg: Aggregator[T], value: T) = {
        agg.count += 1
        add(agg, List(value))
        agg
    }

    def merge[T](agg: Aggregator[T], other: Aggregator[T]) = {
        agg.count += other.count
        add(agg, other.values)
        agg
    }
}
