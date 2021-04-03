package org.example.testing

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.example.Errors.{localDistanceError, localIndexError}
import org.example.{EnvelopeDouble, KnnDistance, KnnEuclideanDistance, KnnResult}
import org.example.Utils.RANDOM
import org.example.construction.{KnnQuery, StatisticsCollector}

object TestingUtils {

    /**
     * Calcula un punto aleatorio dentro del recubrimiento.
     *
     * @param envelope Recubrimiento.
     * @return Punto aleatorio.
     */
    def randomInside(envelope: EnvelopeDouble): Vector = {
        Vectors.dense(envelope.min.zip(envelope.max).map { case (min, max) => min + RANDOM.nextDouble() * (max - min) })
    }

    def randomInside(envelope: EnvelopeDouble, count: Int): Iterable[Vector] = {
        (0 until count).map(_ => randomInside(envelope))
    }

    /**
     * Calcula un punto aleatorio fuera del recubrimiento.
     *
     * @param envelope Recubrimiento.
     * @param factor   Tanto por uno con respecto al tamaño (max - min) de la coordenada.
     * @return Punto aleatorio.
     */
    def randomOutside(envelope: EnvelopeDouble, factor: Double): Vector = {
        Vectors.dense(envelope.min.zip(envelope.max).map { case (min, max) => {
            val d = RANDOM.nextDouble() * factor * (max - min)
            if (RANDOM.nextBoolean()) { // left or right
                min - d
            } else {
                max + d
            }
        }
        })
    }

    def randomOutside(envelope: EnvelopeDouble, factor: Double, count: Int): Iterable[Vector] = {
        (0 until count).map(_ => randomOutside(envelope, factor))
    }

    def doQueries(knnQuery: KnnQuery,
                  data: RDD[(Long, Vector)],
                  distanceEvaluator: KnnDistance,
                  k: Int,
                  queries: Iterable[Vector],
                  statistics: StatisticsCollector,
                  errorCollector: ErrorCollector): Unit = {

        val count = queries.size

        println(s"Queries: $count")
        println()

        queries
            .zipWithIndex
            .foreach { case (query, index) => {
                if (index % 100 == 0) {
                    println(s"    == $index")
                    println()
                }

                doQuery(knnQuery, data, distanceEvaluator, k, query, statistics, errorCollector)
            }
            }
    }

    /**
     * Realiza la consulta de un punto.
     *
     * @param data     Datos originales.
     * @param knnQuery Knn query.
     * @param query    Punto consulta.
     * @param envelope Recubrimiento.
     * @param k        K.
     * @return Errores: (indexError, distanceError, numberOfPoints).
     */
    def doQuery(knnQuery: KnnQuery,
                data: RDD[(Long, Vector)],
                distanceEvaluator: KnnDistance,
                k: Int,
                query: Vector,
                statistics: StatisticsCollector,
                errorCollector: ErrorCollector): Unit = {
        //println(s"Query: $query")

        // Knn calculado por el algoritmo
        // Iterable[(distance: Double, id: Long)]
        val result = knnQuery.query(query, k, statistics)

        // Knn real
        val knnReal = data
            .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
            .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
            .sorted
            .toList

        // Mapa que indica las posiciones reales de cada punto
        // Map[id: Long, (0index: Long, distance: Double)]
        val sc = data.sparkContext
        val setOfIds = sc.broadcast(result.map { case (_, id) => id }.toSet)
        val realMap = data
            .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
            .sortByKey()
            .zipWithIndex
            .filter { case ((_, id), _) => setOfIds.value.contains(id) }
            .take(k)
            .map { case ((distance, id), index) => (id, (index, distance)) }
            .toMap

        // Se calculan los errores de cada punto
        val errors = result
            .zipWithIndex
            .map { case ((distance, id), index) => (id, distance, knnReal(index)._1, index.toLong, realMap(id)._1) }
        errorCollector.collect(errors)
    }

    /*def doQuery_v2(data: RDD[(Long, Vector)],
                   knnQuery: KnnQuery,
                   query: Vector,
                   envelope: EnvelopeDouble, k: Int): (Double, Double, Int) = {
        //val count = data.count()

        // Knn calculado
        // List: (distance: Double, id: Long)
        val result = knnQuery.query(query, k)

        val distance = new KnnEuclideanDistance

        // Knn real
        val knnReal = data
            .map { case (id, point) => (distance.distance(query, point), id) }
            .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
            .sorted
            .toList

        // Mapa que indica las posiciones reales de cada punto
        // Mapa: (id: Long) -> (0index: Long, distance: Double)
        / *val realMap = {
            val sc = data.sparkContext
            val setOfIds = sc.broadcast(result.map { case (_, id) => id }.toSet)

            data
                .map { case (id, point) => (distance.distance(query, point), id) }
                .sortByKey()
                .zipWithIndex
                .filter { case ((_, id), _) => setOfIds.value.contains(id) }
                .take(k)
                .map { case ((d, id), index) => (id, (index, d)) }
                .toMap
        }* /

        val maxDistance = envelope.maxDistance

        //val indexError = globalIndexError(result, realMap, k, count)
        val indexError = 0
        val distanceError = globalDistanceError(result, knnReal, k, maxDistance)

        (indexError, distanceError, result.size)
    }*/
}
