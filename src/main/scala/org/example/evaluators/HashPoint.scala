package org.example.evaluators

import java.util

class HashPoint(private val values: Array[Int]) {

    def this(seq: Seq[Int]) = this(seq.toArray)

    override def hashCode(): Int = {
        util.Arrays.hashCode(values)
    }

    override def equals(other: Any): Boolean = {
        other match {
            case other: HashPoint => util.Arrays.equals(values, other.values)
            case _ => false
        }
    }

    override def toString: String = util.Arrays.toString(values)
}
