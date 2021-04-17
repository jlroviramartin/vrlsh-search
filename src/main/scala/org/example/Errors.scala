package org.example

import scala.collection.immutable.Iterable

trait IndexError {
    def evaluate(index: Long, realIndex: Long) : Double;
}

trait DistanceError {
    def evaluate(distance: Double, realDistance: Double) : Double;
}

trait PointError {
    def evaluate(point: Double, realPoint: Double) : Double;
}

object Errors {

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
