package org.example.testing

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.example.{EnvelopeDouble, KnnDistance, KnnResult, MultiKnnResult}
import org.example.Utils.{RANDOM, time}
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
        val result = knnQuery.query(query, k, distanceEvaluator, statistics)

        // Distancia máxima del resultado: se utiliza para optimizar la búsqueda
        val maxDistanceOfResult = result.map { case (distance, id) => distance }.max

        // Knn real
        /*val knnReal = data
            .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
            .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
            .sorted
            .toList*/

        // Mapa que indica las posiciones reales de cada punto
        // Map[id: Long, (0index: Long, distance: Double)]
        val sc = data.sparkContext
        val setOfIds = sc.broadcast(result.map { case (_, id) => id }.toSet)
        /*val realMap = data
            .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
            .filter { case (distance, id) => (distance <= maxDistance + 0.000001) } // Se añade un Epsilon
            .sortByKey()
            .zipWithIndex
            .filter { case ((_, id), _) => setOfIds.value.contains(id) }
            .take(k)
            .map { case ((distance, id), index) => (id, (index, distance)) }
            .toMap*/

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
            .filter { case (id, (_, _)) => setOfIds.value.contains(id) }
            .collectAsMap()

        //val minDistance
        val maxDistance = data
            .map { case (id, point) => distanceEvaluator.distance(query, point) }
            .max

        // Se calculan los errores de cada punto
        val errors = result
            .groupBy { case (distance, id) => distance }
            .map { case (distance, grouped) => (distance, grouped.map { case (distance, id) => id }) }
            .zipWithIndex
            .flatMap { case ((distance, ids), index) => {
                ids.map(id => {
                    val realDistance = distances(index)
                    val realIndex = realMap(id)._1
                    (id, distance, realDistance, /*maxDistance,*/ index.toLong, realIndex)
                })
            }
            }

        // Solución real: puede tener mas de k elementos, porque tengan igual distancia
        val solution = scala.collection.mutable.Set[Long]()
        groupedByDistanceAndSortedAndIndexed
            .take(k)
            .foreach { case ((distance, ids), index) => {
                if (solution.size < k) {
                    ids.foreach(solution.add(_))
                }
            }
            }

        val calculated = result.map { case (distance, id) => id }.toSet
        val intersection = solution & calculated
        val recall = intersection.size.toDouble / k.toDouble

        errorCollector.collect(errors, recall)
    }


    /*def doRealQueries(data: RDD[(Long, Vector)],
                      distanceEvaluator: KnnDistance,
                      k: Int,
                      queries: Array[(Long, Vector)]): Iterable[(Long, (Array[(Long, Double)], (Long, Double)))] = {
        queries
            .zipWithIndex
            .map { case ((id, query), index) => {
                print(s"$index")
                if (index % 10 == 9) {
                    println()
                } else {
                    print(", ")
                }
                //(id, doRealQuery(data, distanceEvaluator, k, query))

                // -----------------------------------------------------------------------------------------------------

                // Knn real
                val knnReal = data
                    .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
                    .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
                    .sorted
                    .map { case (distance, id) => (id, distance) }
                    .toArray

                val maxDistance = data
                    .map { case (id, point) => (id, distanceEvaluator.distance(query, point)) }
                    .reduce { case ((id1, distance1), (id2, distance2)) =>
                        if (distance1 >= distance2) (id1, distance1)
                        else (id2, distance2)
                    }

                (id, (knnReal, maxDistance))

                // -----------------------------------------------------------------------------------------------------
            }
            }
    }*/

    def doRealQueries_v2(data: RDD[(Long, Vector)],
                         distanceEvaluator: KnnDistance,
                         k: Int,
                         queries: Iterable[(Long, Vector)]): Iterable[(Long, (Array[(Long, Double)], (Long, Double)))] = {

        val sc = data.sparkContext
        val brQueries = sc.broadcast(queries)

        val size = queries.size

        // Knn real
        val multiKnnReal = data
            .map { case (id, point) => (brQueries.value.map { case (idQuery, query) => distanceEvaluator.distance(query, point) }, id) }
            .aggregate(new MultiKnnResult(size))(MultiKnnResult.seqOp(k), MultiKnnResult.combOp(k))
            .array
            .map(knnResult =>
                knnResult
                    .sorted
                    .map { case (distance, id) => (id, distance) }
                    .toArray
            )

        val maxDistances = data
            .map { case (id, point) => (brQueries.value.map { case (idQuery, query) => (id, distanceEvaluator.distance(query, point)) }) }
            .reduce { case (idsDistances1, idsDistances2) =>
                idsDistances1.zip(idsDistances2).map { case ((id1, distance1), (id2, distance2)) => {
                    if (distance1 >= distance2) (id1, distance1)
                    else (id2, distance2)
                }
                }
            }

        //val ids = queries.map { case (id, query) => id }
        //(0 until ids.length).map(i => (ids(i), (knnReal(i), maxDistances(i)))).toArray

        queries.zip(multiKnnReal).zip(maxDistances).map { case (((id, query), knnReal), maxDistance) => (id, (knnReal, maxDistance)) }
    }

    def doRealQuery(data: RDD[(Long, Vector)],
                    distanceEvaluator: KnnDistance,
                    k: Int,
                    query: Vector): (Array[(Long, Double)], (Long, Double)) = {
        // Knn real
        val knnReal = data
            .map { case (id, point) => (distanceEvaluator.distance(query, point), id) }
            .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
            .sorted
            .map { case (distance, id) => (id, distance) }
            .toArray

        val maxDistance = data
            .map { case (id, point) => (id, distanceEvaluator.distance(query, point)) }
            .reduce { case ((id1, distance1), (id2, distance2)) =>
                if (distance1 >= distance2) (id1, distance1)
                else (id2, distance2)
            }

        (knnReal, maxDistance)
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
