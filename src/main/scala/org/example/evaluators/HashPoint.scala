package org.example.evaluators

import java.nio.ByteBuffer
import java.util.{Arrays, Base64}
import scala.collection.Iterable

class HashPoint(val values: Array[Int])
    extends Serializable {

    private def this() = this(new Array[Int](0))

    def this(seq: Int*) = this(seq.toArray)

    def this(seq: Iterable[Int]) = this(seq.toArray)

    def this(hash: HashPoint, index: Int) = this(hash.values ++ Iterator(index))

    override def hashCode: Int = Arrays.hashCode(values)

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: HashPoint => Arrays.equals(values, other.values)
            case _ => false
        }
    }

    override def toString: String = Arrays.toString(values)

    def getName: String = {
        val buff = ByteBuffer.allocate(values.length * 4)
        values.foreach(value => buff.putInt(value))
        val array = buff.array()
        Base64.getEncoder().encodeToString(array)
    }
}
