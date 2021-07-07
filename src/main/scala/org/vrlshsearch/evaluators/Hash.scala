package org.vrlshsearch.evaluators

import java.nio.ByteBuffer
import java.util
import java.util.Base64
import scala.annotation.tailrec
import scala.collection.Iterable

trait Hash extends Serializable {
}

class HashPoint(val values: Array[Int])
    extends Hash with Serializable {

    val hash : Int = util.Arrays.hashCode(values)

    private def this() = this(new Array[Int](0))

    def this(seq: Int*) = this(seq.toArray)

    def this(seq: Iterable[Int]) = this(seq.toArray)

    override def hashCode: Int = hash

    override def equals(obj: Any): Boolean = {
        obj match {
            case other: HashPoint => util.Arrays.equals(values, other.values)
            case _ => false
        }
    }

    override def toString: String = util.Arrays.toString(values)

    def getName: String = {
        val buff = ByteBuffer.allocate(values.length * 4)
        values.foreach(value => buff.putInt(value))
        val array = buff.array()
        Base64.getEncoder.encodeToString(array)
    }
}

object HashPoint {
    @tailrec
    def get(hash: Hash): HashPoint = {
        hash match {
            case hash: HashPoint => hash
            case hash: HashWithIndex => get(hash.hash)
        }
    }

    def getIndex(hash: Hash): Int = {
        hash match {
            case hash: HashPoint => throw new Error()
            case hash: HashWithIndex => hash.index
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
