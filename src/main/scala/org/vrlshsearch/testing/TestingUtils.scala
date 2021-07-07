package org.vrlshsearch.testing

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.vrlshsearch.{BroadcastLookupProvider, EnvelopeDouble, Errors, KnnDistance, KnnResult}
import org.vrlshsearch.Utils.RANDOM
import org.vrlshsearch.construction.KnnQuery
import org.vrlshsearch.statistics.{QualityStatistics, StatisticsCollector}

import scala.collection._

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

    def randomInside(envelope: EnvelopeDouble, count: Int): immutable.Iterable[Vector] = {
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

    def randomOutside(envelope: EnvelopeDouble, factor: Double, count: Int): TraversableOnce[Vector] = {
        (0 until count).map(_ => randomOutside(envelope, factor))
    }

    def doQueriesWithResult(knnQuery: KnnQuery,
                            distanceEvaluator: KnnDistance,
                            k: Int,
                            queries: immutable.Iterable[(Long, Vector)],
                            statistics: StatisticsCollector): immutable.Iterable[(Long, Array[Long])] = {
        queries
            .map { case (id, query) => (id, knnQuery.query(query, k, distanceEvaluator, statistics).map { case (_, id) => id }.toArray) }
    }

    def time_doQueriesWithResult(knnQuery: KnnQuery,
                                 distanceEvaluator: KnnDistance,
                                 k: Int,
                                 queries: immutable.Iterable[(Long, Vector)],
                                 statistics: StatisticsCollector): Unit = {

        queries
            .map { case (_, query) => knnQuery.query(query, k, distanceEvaluator, statistics) }
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
    /*def doQuery(knnQuery: KnnQuery,
                data: RDD[(Long, Vector)],
                distanceEvaluator: KnnDistance,
                k: Int,
                query: Vector,
                statistics: StatisticsCollector,
                errorCollector: ErrorCollector): Unit = {

        // Knn calculado por el algoritmo
        // Iterable[(distance: Double, id: Long)]
        val approximateResult = knnQuery.query(query, k, distanceEvaluator, statistics)

        // Se comprueba cuanto error se ha cometido con la aproximación
        Errors.checkError(
            data,
            approximateResult, // Iterable[(distance: Double, id: Long)]
            distanceEvaluator,
            k,
            query,
            errorCollector)
    }*/

    /*def doFastQueries(knnQuery: KnnQuery,
                      distanceEvaluator: KnnDistance,
                      k: Int,
                      queries: immutable.Iterable[(Long, Vector)],
                      statistics: StatisticsCollector): TraversableOnce[(Long, Array[(Double, Long)])] = {
        queries
            .zipWithIndex
            .map { case ((id, query), index) => {
                if (index % 100 == 0) {
                    println(s"    == $index")
                }

                // Knn calculado por el algoritmo
                // Iterable[(distance: Double, id: Long)]
                val approximateResult = knnQuery.query(query, k, distanceEvaluator, statistics)
                (id, approximateResult.toArray)
            }
            }
    }*/

    def doGroundTruth(data: RDD[(Long, Vector)],
                      distanceEvaluator: KnnDistance,
                      k: Int,
                      queries: Iterator[(Long, Vector)]): Iterator[(Long, Array[Long])] = {
        // Knn real
        queries
            .map { case (queryId, query) => {
                val knnReal = data
                    .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
                    .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
                    .sorted
                    .map { case (_, id) => id }
                    .toArray

                (queryId, knnReal)
            }
            }
    }

    def doMaxDistances(data: RDD[(Long, Vector)],
                       distanceEvaluator: KnnDistance,
                       queries: Iterator[(Long, Vector)]): Iterator[(Long, Double)] = {
        // Máximos
        queries
            .map { case (queryId, queryPoint) => {
                // Máximo de las distancias
                val max = data
                    .map { case (_, point) => distanceEvaluator.distance(queryPoint, point) }
                    .max()

                (queryId, max)
            }
            }
    }

    def checkError(data: RDD[(Long, Vector)],
                   queries: RDD[(Long, Vector)],
                   groundTruth: RDD[(Long, Array[Long])],
                   approximateResult: RDD[(Long, Array[Long])],
                   maxDistances: RDD[(Long, Double)],
                   distanceEvaluator: KnnDistance,
                   k: Int): RDD[QualityStatistics] = {
        assert(!approximateResult.isEmpty)

        val count = data.count()
        val maxIndex = count - 1.toLong

        val lookupProvider = new BroadcastLookupProvider(data)

        queries.join(approximateResult.join(groundTruth.join(maxDistances)))
            .map { case (queryId, (queryPoint, (approx, (ground, maxDistance)))) => {

                // Se obtienen los k primeros elementos teniendo en cuenta que dos o mas puntos pueden estar a igual
                // distancia del punto consulta (puede haber mas de k)
                /*val realResult = ground
                    .map(groundId => (groundId, distanceEvaluator.distance(queryPoint, lookupProvider.lookup(groundId))))
                    .groupBy { case (_, distance) => distance }
                    .map { case (distance, arrayOfIdAndDistance) => (distance, arrayOfIdAndDistance.map { case (id, _) => id }) }
                    .toArray
                    .sortBy { case (distance, _) => distance }
                    .take(k)
                    .flatMap { case (_, arrayOfId) => arrayOfId }*/
                val realResult = ground
                    .map(groundId => (distanceEvaluator.distance(queryPoint, lookupProvider.lookup(groundId)), groundId))
                    .sortBy { case (distance, _) => distance }
                    .map { case (_, id) => id }
                    .take(k)

                // Mapa del id del punto real a su índice y distancia
                val realMap = ground
                    .zipWithIndex
                    .map { case (groundId, index) => (groundId, (index, distanceEvaluator.distance(queryPoint, lookupProvider.lookup(groundId)))) }
                    .toMap

                //if (approx.count() < k)
                //if (!approx.forall(id => realMap.contains(id))) {
                //    println(s"El punto $queryId NO encuentra solución dentro de groundTruth")
                //}

                val errors = approx
                    .filter(id => id >= 0)
                    .zipWithIndex
                    .map { case (id, index) => {
                        val distance = distanceEvaluator.distance(queryPoint, lookupProvider.lookup(id))

                        if (realMap.contains(id)) {
                            val (realIndex, realDistance) = realMap(id)

                            val indexError = Errors.localIndexError(index, realIndex, maxIndex)
                            val distanceErrorNorm = Errors.localDistanceError(distance, realDistance, count)
                            val distanceError = Errors.distanceError(distance, realDistance)
                            val approxRatio = Errors.approximationRatio(distance, realDistance)

                            (indexError, distanceErrorNorm, distanceError, approxRatio)
                        } else {
                            // Se asume error máximo
                            val indexError = Errors.localIndexError(index, maxIndex, maxIndex)
                            val distanceErrorNorm = Errors.localDistanceError(distance, maxDistance, count)
                            val distanceError = Errors.distanceError(distance, maxDistance)
                            val approxRatio = Errors.approximationRatio(distance, maxDistance)

                            (indexError, distanceErrorNorm, distanceError, approxRatio)
                        }
                    }
                    }

                // Se calcula la suma de todos los errores...
                val (sumIndexError: Double, sumDistanceErrorNorm: Double, sumDistanceError: Double, sumApproxRatio: Double, size: Int) = errors
                    .foldLeft((0.0, 0.0, 0.0, 0.0, 0)) {
                        case (
                            (accIndexError, accDistanceErrorNorm, accDistanceError, accApproxRatio, accCount),
                            (indexError: Double, distanceErrorNorm: Double, distanceError: Double, approxRatio: Double)) =>

                            // Se calcula el acumulado
                            (accIndexError + indexError,
                                accDistanceErrorNorm + distanceErrorNorm,
                                accDistanceError + distanceError,
                                accApproxRatio + approxRatio,
                                accCount + 1)
                    }

                // .. y su media
                val (avgIndexError, avgDistanceErrorNorm, avgDistanceError, avgApproxRatio) = (sumIndexError / size, sumDistanceErrorNorm / size, sumDistanceError / size, sumApproxRatio / size)

                // Se calcula el ratio de aproximación
                val realDistances = realResult
                    .map(groundId => distanceEvaluator.distance(queryPoint, lookupProvider.lookup(groundId)))
                val approxDistances = approx
                    .map(groundId => distanceEvaluator.distance(queryPoint, lookupProvider.lookup(groundId)))
                val c = Errors.evaluateApproximateRatio(k, approxDistances, realDistances)

                // Se calcula el recall
                val recall = Errors.evaluateRecall(k, approx, realResult)

                // Se calcula la precisión
                val apk = Errors.evaluateAPK(k, approx, realResult)

                val statistics = new QualityStatistics
                statistics.avgIndexError = avgIndexError
                statistics.avgDistanceErrorNorm = avgDistanceErrorNorm
                statistics.avgDistanceError = avgDistanceError
                statistics.approxRatio = c
                statistics.recall = recall
                statistics.avgPrecision = apk
                statistics
            }
            }
    }
}
