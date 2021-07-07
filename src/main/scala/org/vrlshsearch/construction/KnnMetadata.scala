package org.vrlshsearch.construction

import org.vrlshsearch.evaluators.Hasher

import scala.collection.mutable

/**
 * Esta clase almacena los meta-datos necesarios para calcular el knn:
 * <ul>
 * <li>lista de radios ordenados de menos a mayor</li>
 * <li>mapa de radio a Hasher</li>
 * </ul>
 */
@SerialVersionUID(4003847730739634848l)
class KnnMetadata(var hasherMap: mutable.Map[Double, Hasher]) extends Serializable {

    def this() = this(new mutable.HashMap)

    def put(radius: Double, hasher: Hasher): Unit = hasherMap.put(radius, hasher)

    def radiuses: Seq[Double] = hasherMap.keys.toSeq.sorted

    def getHasher(radius: Double): Option[Hasher] = hasherMap.get(radius)
}
