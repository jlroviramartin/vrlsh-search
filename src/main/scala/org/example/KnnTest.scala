package org.example

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.example.Utils.time
import org.example.construction.{KnnConstructionAlgorithm, KnnQuery, MyKnnQuerySerializator}
import org.example.Utils.RANDOM

import java.nio.file.{Files, Path}
import scala.collection.Seq
import scala.reflect.io.Directory
import org.example.Errors._

object KnnTest {

    /**
     * Calcula un punto aleatorio dentro del recubrimiento.
     *
     * @param envelope Recubrimiento.
     * @return Punto aleatorio.
     */
    def randomInside(envelope: EnvelopeDouble): Vector = {
        Vectors.dense(envelope.min.zip(envelope.max).map { case (min, max) => min + RANDOM.nextDouble() * (max - min) })
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

    def testSet1(envelope: EnvelopeDouble,
                 data: RDD[(Long, Vector)],
                 baseDirectory: Path,
                 desiredSize: Int): Unit = {
        println(f"Min of axes: ${envelope.sizes.min}%1.5f")
        println()

        println(f"Max of axes: ${envelope.sizes.max}%1.5f")
        println()

        println(f"Max distance approx.: ${envelope.maxDistance}%1.5f")
        println()

        // Se construye/deserializa el objeto knnQuery
        val knnQuery: KnnQuery = if (false) {
            val distance = new KnnEuclideanDistance
            val knnQuery = {
                // Se limpian los datos antiguos
                val directory = new Directory(baseDirectory.toFile)
                directory.deleteRecursively()

                new KnnConstructionAlgorithm(desiredSize, baseDirectory.toString, distance).build(data)
            }

            DataStore.kstore(
                baseDirectory.resolve("KnnQuery.dat"),
                knnQuery.getSerializable())

            knnQuery
        } else {
            val sc = data.sparkContext

            val knnQuery = DataStore.kload(
                baseDirectory.resolve("KnnQuery.dat"),
                classOf[MyKnnQuerySerializator]).get(sc)
            knnQuery
        }

        val k = 10
        val testExact = 50
        val testInside = 500
        val testOutside = 500
        val factorOutside = 0.5

        println("## Exact points")
        println()

        val errorsExact = (0 until testExact).map(index => {
            println(s"----- $index -----")
            println()

            val sample = data.takeSample(withReplacement = false, 1, RANDOM.nextLong)
            val query = sample(0)._2
            KnnTest.doQuery(data, knnQuery, query, envelope, k)
        })
        showAverageOfErrors(errorsExact, k)

        println("## Inside points")
        println()

        // Se calculan n puntos aleatorios dentro del recubrimiento
        val errorsInside = (0 until testInside).map(index => {
            println(s"----- $index -----")
            println()

            KnnTest.doQuery(data, knnQuery, KnnTest.randomInside(envelope), envelope, k)
        })
        showAverageOfErrors(errorsInside, k)

        println("## Outside points")
        println()

        // Se calculan n puntos aleatorios dentro del recubrimiento
        val errorsOutside = (0 until testOutside).map(index => {
            println(s"----- $index -----")
            println()

            KnnTest.doQuery(data, knnQuery, KnnTest.randomOutside(envelope, factorOutside), envelope, k)
        })
        showAverageOfErrors(errorsOutside, k)
    }

    def testSet2(data: RDD[(Long, Vector)],
                 baseDirectory: Path,
                 desiredSize: Int,
                 k: Int,
                 testPercentage: Double): Unit = {

        val trainTest = data.randomSplit(Array(100 - testPercentage, testPercentage), seed = RANDOM.nextLong())
        val train = trainTest(0)
        val test = trainTest(1)

        println(s"Train set: ${train.count()}")
        println(s"Test set: ${test.count()}")

        // Se calcula el recubrimiento
        val envelope = train
            .map { case (_, point) => point }
            .aggregate(EnvelopeDouble.EMPTY)(
                EnvelopeDouble.seqOp,
                EnvelopeDouble.combOp)

        println(f"Min of axes: ${envelope.sizes.min}%1.5f")
        println()

        println(f"Max of axes: ${envelope.sizes.max}%1.5f")
        println()

        println(f"Max distance approx.: ${envelope.maxDistance}%1.5f")
        println()

        val dir = baseDirectory.resolve(testPercentage.toString)
        Files.createDirectories(dir)
        println(s"Directory: $dir")

        // Se construye/deserializa el objeto knnQuery
        val knnQuery: KnnQuery = if (false) {
            val distance = new KnnEuclideanDistance

            val knnQuery = {
                // Se limpian los datos antiguos
                val directory = new Directory(dir.toFile)
                directory.deleteRecursively()

                new KnnConstructionAlgorithm(desiredSize, dir.toString, distance).build(train)
            }

            DataStore.kstore(
                dir.resolve("KnnQuery.dat"),
                knnQuery.getSerializable())

            knnQuery
        } else {
            val sc = data.sparkContext

            val knnQuery = DataStore.kload(
                dir.resolve("KnnQuery.dat"),
                classOf[MyKnnQuerySerializator]).get(sc)

            knnQuery
        }

        val errors = test.collect().zipWithIndex.map { case (query, index) => {
            println(s"$index")
            KnnTest.doQuery2(train, knnQuery, query._2, envelope, k)
        }
        }
        showAverageOfErrors(errors, k)
    }

    /**
     * Realiza la consulta de un punto.
     *
     * @param data     Datos originales.
     * @param knnQuery Knn query.
     * @param query    Punto consulta.
     * @param envelope Recubrimiento.
     * @param k        K.
     * @return Errores: (indexError, distanceError).
     */
    def doQuery(data: RDD[(Long, Vector)],
                knnQuery: KnnQuery,
                query: Vector,
                envelope: EnvelopeDouble, k: Int): (Double, Double, Int) = {
        //println(s"Query: $query")

        val count = data.count()

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
        val realMap = {
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
        }

        val maxDistance = envelope.maxDistance

        // Se muestra el resultado como una tabla md
        if (false) {
            println("| distance | id | index | realIndex | index error | distance error |")
            println("| -: | -: | -: | -: | -: | -: |")
            result
                .zipWithIndex
                .map { case ((distance, id), index) => (id, distance, index, realMap(id)._1) }
                .foreach { case (id, distance, index, realIndex) =>
                    val indexError = localIndexError(index, realIndex, k, count)
                    val distanceError = localDistanceError(distance, knnReal(index)._1, maxDistance)

                    println(f"| $distance | $id | $index | $realIndex | $indexError%1.5f | $distanceError%1.5f |")
                }
            println()
        }

        val indexError = globalIndexError(result, realMap, k, count)
        val distanceError = globalDistanceError(result, knnReal, k, maxDistance)

        println(f"Index error avg.: $indexError%1.5f")
        println()

        println(f"Distance error avg.: $distanceError%1.5f")
        println()

        (indexError, distanceError, result.size)
    }

    def doQuery2(data: RDD[(Long, Vector)],
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
        /*val realMap = {
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
        }*/

        val maxDistance = envelope.maxDistance

        //val indexError = globalIndexError(result, realMap, k, count)
        val indexError = 0
        val distanceError = globalDistanceError(result, knnReal, k, maxDistance)

        (indexError, distanceError, result.size)
    }

    def showAverageOfErrors(data: Seq[(Double, Double, Int)], k: Int): Unit = {
        val len = data.length
        val indexError = (data.map { case (indexError, _, _) => indexError }.sum) / len
        val distanceError = (data.map { case (_, distanceError, _) => distanceError }.sum) / len
        val numberOfSemiFailures = data.count { case (_, _, length) => length > 0 && length < k }
        val numberOfFailures = data.count { case (_, _, length) => length == 0 }

        println(f"Index error avg.: $indexError%1.5f")
        println()

        println(f"Distance error avg.: $distanceError%1.5f")
        println()

        if (numberOfSemiFailures > 0) {
            println(s"No existe solución completa: $numberOfSemiFailures")
            println()
        }

        if (numberOfFailures > 0) {
            println(s"No existe NINGUNA solución: $numberOfFailures")
            println()
        }
    }
}
