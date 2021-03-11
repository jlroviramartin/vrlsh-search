package org.example.construction

import org.example.evaluators.Hasher

import scala.collection.mutable

class KnnMetadata extends Serializable {

    val hasherMap: mutable.Map[Double, Hasher] = new mutable.HashMap()

    def put(radius: Double, hasher: Hasher) = {
        hasherMap.put(radius, hasher)
    }

    def radius: Seq[Double] = hasherMap.keys.toSeq.sorted

    def getHasher(radius: Double) = hasherMap.get(radius)
}
