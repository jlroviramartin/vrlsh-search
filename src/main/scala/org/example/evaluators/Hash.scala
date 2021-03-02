package org.example.evaluators

import java.nio.ByteBuffer
import java.util.{Arrays, Base64}
import scala.collection.Iterable

trait Hash extends Serializable {
}

class HashPoint(val values: Array[Int])
    extends Hash with Serializable {

    private def this() = this(new Array[Int](0))

    def this(seq: Int*) = this(seq.toArray)

    def this(seq: Iterable[Int]) = this(seq.toArray)

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

object HashPoint {
    def get(hash: Hash): HashPoint = {
        hash match {
            case hash: HashPoint => hash
            case hash: HashWithIndex => get(hash.hash)
        }
    }
}

class HashWithIndex(val index: Int, val hash: Hash)
    extends Hash {

    {
        assert(hash.isInstanceOf[HashPoint])
    }

    override def hashCode: Int = index.hashCode() ^ hash.hashCode

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: HashWithIndex => index == other.index && hash.equals(other.hash)
            case _ => false
        }
    }

    override def toString: String = s"($index, $hash)"
}
