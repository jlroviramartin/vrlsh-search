package org.example.construction

import org.example.evaluators.Hasher

import scala.collection.mutable

@SerialVersionUID(4003847730739634848l)
class KnnMetadata(var hasherMap: mutable.Map[Double, Hasher]
                 ) extends Serializable {

    def this() = this(new mutable.HashMap)

    def put(radius: Double, hasher: Hasher): Unit = hasherMap.put(radius, hasher)

    def radiuses: Seq[Double] = hasherMap.keys.toSeq.sorted

    def getHasher(radius: Double): Option[Hasher] = hasherMap.get(radius)
}
