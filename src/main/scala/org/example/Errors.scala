package org.example

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.testing.ErrorCollector

import scala.collection.immutable.Iterable

trait IndexError {
    def evaluate(index: Long, realIndex: Long): Double
}

trait DistanceError {
    def evaluate(distance: Double, realDistance: Double): Double
}

trait PointError {
    def evaluate(point: Double, realPoint: Double): Double
}

object Errors {

    /**
     * Se calcula el error cometido por la aproximación.
     *
     * @param data
     * @param result
     * @param distanceEvaluator
     * @param k
     * @param query
     * @param errorCollector
     */
    def checkError(data: RDD[(Long, Vector)],
                   approximateResult: Iterable[(Double, Long)], // Iterable[(distance: Double, id: Long)]
                   distanceEvaluator: KnnDistance,
                   k: Int,
                   query: Vector,
                   errorCollector: ErrorCollector): Unit = {
        val sc = data.sparkContext

        if (approximateResult.isEmpty) {
            val errors = Array[(Long, Double, Double, Double, Long, Long)]()
            val recall = 0
            val apk = 0
            errorCollector.collect(errors, recall, apk)
            return
        }

        // Distancia máxima del resultado: se utiliza para optimizar la búsqueda
        val maxDistanceOfResult = approximateResult.map { case (distance, id) => distance }.max

        // Mapa que indica las posiciones reales de cada punto
        // Map[id: Long, (0index: Long, distance: Double)]
        val bsetOfIds = sc.broadcast(approximateResult.map { case (_, id) => id }.toSet)

        val filteredByDistance = data
            .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
            .filter { case (distance, id) => distance <= maxDistanceOfResult }
            .cache()

        val distances = filteredByDistance
            .map { case (distance, id) => distance }
            .distinct()
            .sortBy(distance => distance)
            .take(k)

        val groupedByDistanceAndSortedAndIndexed = filteredByDistance
            .groupByKey() // Se agrupa por distancia: igual distancia igual indice
            .sortByKey()
            .zipWithIndex
            .cache()

        val realMap = groupedByDistanceAndSortedAndIndexed
            .flatMap { case ((distance, ids), index) => ids.map(id => (id, (index, distance))) }
            .filter { case (id, (_, _)) => bsetOfIds.value.contains(id) }
            .collectAsMap()

        val maxDistance = data.map { case (id, point) => distanceEvaluator.distance(query, point) }.max()

        // Se calculan los errores de cada punto
        val errors = approximateResult
            .groupBy { case (distance, _) => distance }
            .toList
            .sortBy { case (distance, _) => distance }
            .map { case (distance, grouped) => (distance, grouped.map { case (_, id) => id }) }
            .zipWithIndex
            .flatMap { case ((distance, ids), index) => {
                ids.map(id => {
                    val realDistance = distances(index)
                    val realIndex = realMap(id)._1

                    (id,
                        distance, realDistance, maxDistance,
                        index.toLong, realIndex)
                })
            }
            }

        // DEBUG -------------------------------------------------------------------------------------------------------
        /*println("Real knn")
        groupedByDistanceAndSortedAndIndexed
            .foreach { case ((distance, points), index) => {
                println(s"$distance - $index - [${points.mkString("; ")}]")
            }
            }*/

        /*println("Approximate knn")
        val count = data.count()
        errors
            .foreach { case (id, distance, realDistance, maxDistance, index, realIndex) => {
                val indexError = Errors.localIndexError(index, realIndex, count)
                val error = Errors.localDistanceError(distance, realDistance, maxDistance)
                println(f"$distance%1.5f [$realDistance%1.5f] - $realIndex - $id - $indexError%1.5f - $error%1.5f")
            }
            }*/
        // DEBUG -------------------------------------------------------------------------------------------------------

        // Solución real: puede tener mas de k elementos, porque tengan igual distancia
        /*val solution = scala.collection.mutable.Set[Long]()
        groupedByDistanceAndSortedAndIndexed
            .take(k)
            .foreach { case ((distance, ids), index) => {
                if (solution.size < k) {
                    ids.foreach(solution.add(_))
                }
            }
            }*/
        val solution = groupedByDistanceAndSortedAndIndexed
            .flatMap { case ((distance, ids), index) => ids }.take(k)
        val calculated = approximateResult.map { case (_, id) => id }.toArray

        val intersection = solution.toSet & calculated.toSet
        val recall = intersection.size.toDouble / k.toDouble

        val apk = (1 / k.toDouble) * (1 to k).map((i: Int) => {
            if (i < calculated.length && solution.contains(calculated(i))) {
                val intersection_i = solution.toSet & calculated.take(i).toSet
                val recall_i = intersection_i.size.toDouble / i.toDouble
                recall_i
            } else {
                0
            }
        }).sum

        errorCollector.collect(errors, recall, apk)
    }

    /**
     * Calcula el error por índice para un punto.
     *
     * @param index     Índice según el algoritmo.
     * @param realIndex Índice real del punto.
     * @param count     Número de puntos.
     * @return Error por índice.
     */
    def localIndexError(index: Long, realIndex: Long, count: Long): Double = {
        Math.abs(realIndex - index).toDouble / (count - 1).toDouble
    }

    /**
     * Calcula el error por distancia para un punto.
     *
     * @param distance    Distancia según el algoritmo.
     * @param realIndex   Distancia real del punto.
     * @param maxDistance Distancia máxima.
     * @return Error por distancia.
     */
    def localDistanceError(distance: Double, realDistance: Double, maxDistance: Double): Double = {
        Math.abs(distance - realDistance) / maxDistance
    }

    def distanceError(distance: Double, realDistance: Double): Double = {
        Math.abs(distance - realDistance)
    }

    def approximationRatio(distance: Double, realDistance: Double): Double = {
        Math.abs(distance / realDistance)
    }

    /**
     * Calcula el error por índice medio.
     *
     * @param result  Resultado del algoritmo.
     * @param realMap Mapa de los puntos al indice real.
     * @param k       K.
     * @param count   Número de puntos
     * @return Error por índice.
     */
    def globalIndexError(result: Iterable[(Double, Long)], realMap: Map[Long, (Long, Double)], k: Int, count: Long): Double = {
        val sum = result
            .zipWithIndex
            .map { case ((distance, id), index) => realMap(id) match {
                case (realIndex, distance) => (realIndex - index).toDouble / (count - k).toDouble
            }
            }
            .sum
        // Average
        sum / k
    }

    /**
     * Calcula el error por distancia medio.
     *
     * @param result      Resultado del algoritmo.
     * @param real        Resultado real.
     * @param k           K.
     * @param maxDistance Distancia máxima.
     * @return Error por distancia.
     */
    def globalDistanceError(result: Iterable[(Double, Long)], real: List[(Double, Long)], k: Int, maxDistance: Double): Double = {
        val sum = result
            .zipWithIndex
            .map { case ((distance, id), index) => (distance - real(index)._1) / maxDistance }
            .sum
        // Average
        sum / k
    }
}
