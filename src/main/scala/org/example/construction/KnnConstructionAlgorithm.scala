package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, DataStore, Distance, EnvelopeDouble, HashOptions, KnnDistance, KnnEuclideanSquareDistance, KnnResult, Utils}
import org.example.Utils.time
import org.example.evaluators.{DefaultHasher, Hash}
import Utils._
import org.apache.spark.SparkContext

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.file.Paths
import scala.collection.immutable.TreeMap

class KnnConstructionAlgorithm(val desiredSize: Int,
                               val baseDirectory: String,
                               val distance: KnnDistance)
    extends KnnConstruction {

    val min: Double = desiredSize * Utils.MIN_TOLERANCE
    val max: Double = desiredSize * Utils.MAX_TOLERANCE
    val radiusMultiplier: Double = 1.4

    override def build(data: RDD[(Long, Vector)]): KnnQuery = {

        // Contexto de Spark
        val sc = data.sparkContext

        val dimension = data.first()._2.size

        //val (hasher, hashOptions, radius) = time(s"desiredSize = $desiredSize tolerance = ($min, $max)") {
        //    Hasher.getHasherForDataset(data, dimension, desiredSize)
        //} // 2 min
        val radius = 0.1
        val hashOptions = new HashOptions(dimension, 16, 14)

        var currentHasher = hashOptions.newHasher()
        var bcurrentHasher = sc.broadcast(currentHasher)

        val lookupProvider = time("    Se hace broadcast de los datos") {
            new BroadcastLookupProvider(data)
        }

        var currentIndices = data.map { case (id, _) => id }
        var currentRadius = radius
        var iteration = 0

        // Resultado
        var result: RDD[(Double, Hash, Long)] = sc.emptyRDD
        val hasherMap = new KnnMetadata()

        // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
        var bucketCondition = (size: Int) => size >= min && size <= max

        while (!time("Se comprueba si está vacío") {
            currentIndices.isEmpty()
        }) {
            time("Iteración") {
                println(s"  R$iteration: $currentRadius")
                println(s"  puntos restantes: ${currentIndices.count()}")

                // Se guarda currentRadius porque se modifica al final del while y produce si se utiliza dentro de
                // operaciones en RDDs
                val savedRadius = currentRadius

                val bsavedHasher = bcurrentHasher

                val currentData = currentIndices
                    .flatMap(id => bsavedHasher.value.hash(lookupProvider.lookup(id), savedRadius).map(hash => (hash, id)))

                if (time("    Se comprueba si todos son BASE") {
                    forAll(currentData) { case (hash, _) => isBaseHashPoint(hash) }
                }) {
                    println("    ===== Todos son BASE =====")

                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                    // En la iteración actual TODOS forman buckets
                    // -----> bucketCondition = _ => true
                    ////////////////////////////////////////////////////////////////////////////////////////////////////
                }

                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                if (iteration > 80) {
                    bucketCondition = _ => true
                }
                ////////////////////////////////////////////////////////////////////////////////////////////////////////

                // Se calculan los buckets (hash) y el número de puntos en cada uno
                val hashWithNumPoints = currentData
                    .aggregateByKey(0)(
                        { case (numPoints, _) => numPoints + 1 },
                        (numPoints1, numPoints2) => numPoints1 + numPoints2)
                    .cache()

                // Se muestran estadísticas: cuantos buckets tienen un cierto número de puntos
                println("    Estadísticas: Número de puntos - Número de buckets")
                showStatistics(hashWithNumPoints, bucketCondition)

                // Se calculan los datos que se van a usar para los buckets
                val dataForBuckets = currentData
                    .subtractByKey(
                        hashWithNumPoints.filter { case (_, numPoints) => !bucketCondition(numPoints) }
                    ).cache()

                if (!dataForBuckets.isEmpty()) {
                    // Se muestran los envelopes
                    //println("    Envelopes");
                    //showEnvelopes(dataForBuckets, lookupProvider)

                    // Se calcula el tamaño máximo de los envelopes
                    showMinMaxEnvelope(dataForBuckets, lookupProvider)

                    // Se actualiza el resultado
                    result = result.union(dataForBuckets.map { case (hash, id) => (savedRadius, hash, id) })
                    hasherMap.put(savedRadius, bsavedHasher.value)

                    // Se calculan los nuevos índices
                    val usedIndices = dataForBuckets
                        .map { case (_, id) => id }
                    currentIndices = currentIndices.subtract(usedIndices)
                }

                // Se incrementa el radio
                currentRadius = currentRadius * radiusMultiplier

                // Se crea un nuevo hasher
                println("    Se crea un nuevo hasher")
                currentHasher = hashOptions.newHasher()
                if (bcurrentHasher != null) {
                    bcurrentHasher.destroy()
                }
                bcurrentHasher = sc.broadcast(currentHasher)

                iteration = iteration + 1
            }
        }

        println("Se finaliza!")

        if (bcurrentHasher != null) bcurrentHasher.destroy()

        println(s"Se almacena el resultado: $baseDirectory")

        val dataFile = Paths.get(baseDirectory, "data").toString
        val modelFile = Paths.get(baseDirectory, "model").toString
        data.saveAsObjectFile(dataFile)
        result.saveAsObjectFile(modelFile)

        new MyKnnQuery(baseDirectory, data, result, hasherMap, lookupProvider, distance)
    }

    private def showStatistics(hashWithNumPoints: RDD[(Hash, Int)], bucketCondition: Int => Boolean): Unit = {
        time("    Se actualizan las estadísticas") {
            hashWithNumPoints
                .filter { case (_, numPoints) => bucketCondition(numPoints) }
                .map { case (_, numPoints) => (numPoints, 1) }
                .reduceByKey((count1, count2) => count1 + count2)
                .sortByKey()
                .foreach { case (numPoints, count) => println(s"      $numPoints - $count") }
        }
    }

    private def showEnvelopes(dataForBuckets: RDD[(Hash, Long)], lookupProvider: BroadcastLookupProvider): Unit = {
        // Se calculan el tamaño de los buckets
        dataForBuckets
            .aggregateByKey(EnvelopeDouble.EMPTY)(
                (envelope, id) => EnvelopeDouble.seqOp(envelope, lookupProvider.lookup(id)),
                EnvelopeDouble.combOp)
            .map { case (hash, envelope) => (hash, envelope.sizes.max) } // Max of the sizes of the envelope
            .foreach { case (hash, value) => {
                println(s"    $hash - $value")
            }
            }
    }

    private def showMinMaxEnvelope(dataForBuckets: RDD[(Hash, Long)], lookupProvider: BroadcastLookupProvider): Unit = {
        // Se calculan el tamaño de los buckets
        val maxEnvelopes = dataForBuckets
            .aggregateByKey(EnvelopeDouble.EMPTY)(
                (envelope, id) => EnvelopeDouble.seqOp(envelope, lookupProvider.lookup(id)),
                EnvelopeDouble.combOp)
            .map { case (_, envelope) => (envelope.sizes.min, envelope.sizes.max) } // (min, max) of the sizes of the envelope
            .cache()

        if (!maxEnvelopes.isEmpty()) {
            val min = maxEnvelopes.map(_._1).min()
            val max = maxEnvelopes.map(_._2).max()
            println(s"    Envelopes Min: $min Max: $max")
        }
    }
}

class MyKnnQuery(val baseDirectory: String,
                 val data: RDD[(Long, Vector)],
                 val result: RDD[(Double, Hash, Long)],
                 val hasherMap: KnnMetadata,
                 val lookupProvider: BroadcastLookupProvider,
                 val distance: KnnDistance)
    extends KnnQuery with Serializable {

    def query(point: Vector, k: Int): List[(Double, Long)] = {
        val queries = hasherMap.radius
            .map(radius => (radius, hasherMap.getHasher(radius)))
            .flatMap {
                case (radius, Some(hasher)) => hasher.hash(point, radius).map(hash => (radius, hash))
            }
            .toSet

        val sc = result.sparkContext
        val bqueries = sc.broadcast(queries)
        val bpoint = sc.broadcast(point)

        result
            .filter {
                case (radius, hash, _) => bqueries.value.contains((radius, hash))
            }
            .map {
                case (_, _, id) => (distance.distance(bpoint.value, lookupProvider.lookup(id)), id)
            }
            .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
            .sorted.toList
    }

    def getSerializable(): KnnQuerySerializable = {
        new MyKnnQuerySerializator(this)
    }

}

class MyKnnQuerySerializator(var baseDirectory: String,
                             var hasherMap: KnnMetadata,
                             var distance: KnnDistance)
    extends KnnQuerySerializable {

    def this(query: MyKnnQuery) = {
        this(query.baseDirectory, query.hasherMap, query.distance)
    }

    def get(sc: SparkContext): MyKnnQuery = {
        val dataFile = Paths.get(baseDirectory, "data").toString
        val modelFile = Paths.get(baseDirectory, "model").toString

        val data = sc.objectFile[(Long, Vector)](dataFile)
        val result = sc.objectFile[(Double, Hash, Long)](modelFile)

        val lookupProvider = new BroadcastLookupProvider(data)

        new MyKnnQuery(baseDirectory, data, result, hasherMap, lookupProvider, distance)
    }
}
