package org.example.evaluators

import java.util
import scala.collection.Iterable

class HashPoint(private val values: Array[Int])
    extends Serializable {

    def this(seq: Iterable[Int]) = this(seq.toArray)

    def this(hash: HashPoint, index: Int) = this(hash.values ++ Iterator(index))

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
