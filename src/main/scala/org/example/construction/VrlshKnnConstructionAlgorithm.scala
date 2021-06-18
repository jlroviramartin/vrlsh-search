package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, DataStore, EnvelopeDouble, KnnDistance, KnnResult, Utils}
import org.example.Utils.time
import org.example.evaluators.{EuclideanHasher, Hash, HasherFactory}
import org.apache.spark.SparkContext
import Utils._
import org.example.statistics.{GeneralStatistics, EvaluationStatistics, StatisticsCollector}

import java.nio.file.{Path, Paths}
import scala.collection.immutable.Iterable
import scala.util.control.Breaks

class VrlshKnnConstructionAlgorithm(val hasherFactory: HasherFactory,
                                    val desiredSize: Int,
                                    val baseDirectory: String)
    extends KnnConstruction {

    val min: Double = desiredSize * Utils.MIN_TOLERANCE
    val max: Double = desiredSize * Utils.MAX_TOLERANCE
    val radiusMultiplier: Double = 1.4

    override def build(data: RDD[(Long, Vector)]): VrlshKnnQuery = {

        // Contexto de Spark
        val sc = data.sparkContext

        val dimension = data.first()._2.size

        println("Se evalúan los hashers")

        val (hasher, hashOptions, radius) = hasherFactory.getHasherForDataset(data, dimension, desiredSize)

        var currentHasher = hasher
        //var currentHasher = hashOptions.newHasher()
        //var bcurrentHasher = sc.broadcast(currentHasher)

        println("Inicio broadcast")

        val lookupProvider = new BroadcastLookupProvider(data)

        println("Fin broadcast")

        var currentIndices = data.map { case (id, _) => id }
        /*----- .cache()*/
        var currentRadius = radius
        var iteration = 0

        val maxIterations = 100

        // Resultado
        var result: RDD[(Double, Hash, List[Long])] = sc.emptyRDD
        val hasherMap = new KnnMetadata()

        // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
        val M = max
        val m = min
        var bucketCondition = (size: Int) => size >= m && size <= M

        var count = currentIndices.count()

        while (count > 0) {
            println(s"  R$iteration: $currentRadius")
            println(s"  puntos restantes: $count")

            // Se guarda currentRadius porque se modifica al final del while y produce si se utiliza dentro de
            // operaciones en RDDs
            val savedRadius = currentRadius

            val savedHasher = currentHasher
            //val bsavedHasher = bcurrentHasher

            println("--- evaluate and persist currentData ---")

            val currentData = currentIndices
                .flatMap(id => savedHasher.hash(lookupProvider.lookup(id), savedRadius).map(hash => (hash, id)))
            /*----- .cache()*/

            if (forAll(currentData) { case (hash, _) => isBaseHashPoint(hash) }) {
                println("    ===== Todos son BASE =====")

                ////////////////////////////////////////////////////////////////////////////////////////////////////
                // En la iteración actual TODOS forman buckets
                // -----> bucketCondition = _ => true
                ////////////////////////////////////////////////////////////////////////////////////////////////////
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////
            var isLast = false
            if (count < min) {
                println("    ===== No hay puntos suficientes =====")
                bucketCondition = _ => true
                isLast = true
            } else if (iteration > maxIterations) {
                println("    ===== Se ha alcanzado el máximo de iteraciones =====")
                bucketCondition = _ => true
                isLast = true
            }
            ////////////////////////////////////////////////////////////////////////////////////////////////////////

            val savedBucketCondition = bucketCondition

            // Se calculan los buckets (hash) y el número de puntos en cada uno
            val hashWithNumPoints = currentData
                .aggregateByKey(0)(
                    { case (numPoints, _) => numPoints + 1 },
                    (numPoints1, numPoints2) => numPoints1 + numPoints2)
                .cache()

            // Se muestran estadísticas: cuantos buckets tienen un cierto número de puntos
            //showStatistics(hashWithNumPoints, bucketCondition)
            //if (isLast) {
            //    showStatistics(hashWithNumPoints, bucketCondition)
            //}
            //showBucketCount(hashWithNumPoints, bucketCondition)

            println("Se calculan los datos que se van a usar para los buckets")

            // Se calculan los datos que se van a usar para los buckets
            val dataForBuckets = currentData
                .subtractByKey(
                    hashWithNumPoints.filter { case (_, numPoints) => !savedBucketCondition(numPoints) }
                )
            /*----- .cache()*/

            println("dataForBuckets.isEmpty")

            if (!dataForBuckets.isEmpty()) {
                // Se muestran los envelopes
                //println("    Envelopes");
                //showEnvelopes(dataForBuckets, lookupProvider)

                println("Se calcula el tamaño mínimo/máximo de los envelopes")

                // Se calcula el tamaño mínimo/máximo de los envelopes
                //----- showMinMaxEnvelope(dataForBuckets, lookupProvider)

                println("Se actualiza el resultado")

                // Se actualiza el resultado
                result = result.union(dataForBuckets
                    .aggregateByKey(List[Long]())(
                        { case (list, id) => id :: list },
                        { case (list1, list2) => list1 ++ list2 }
                    )
                    .map { case (hash, ids) => (savedRadius, hash, ids) })
                hasherMap.put(savedRadius, savedHasher)

                println("Se calculan los nuevos índices")

                println("--- evaluate and persist currentIndices ---")

                // Se calculan los nuevos índices
                val usedIndices = dataForBuckets
                    .map { case (_, id) => id }
                currentIndices = currentIndices.subtract(usedIndices)
                    .cache()

                count = currentIndices.count()
            }

            println("Se incrementa el radio")

            // Se incrementa el radio
            currentRadius = currentRadius * radiusMultiplier

            // Se crea un nuevo hasher
            //println("  Se crea un nuevo hasher")
            currentHasher = hashOptions.newHasher()
            //if (bcurrentHasher != null) {
            //    bcurrentHasher.destroy()
            //}
            //bcurrentHasher = sc.broadcast(currentHasher)

            iteration = iteration + 1

            println("--------------------------------------------------")

        }

        println("Se finaliza!")

        //if (bcurrentHasher != null) {
        //    bcurrentHasher.destroy()
        //}

        println(s"Se almacena el resultado: $baseDirectory")

        val dataFile = Paths.get(baseDirectory, "data").toString
        val modelFile = Paths.get(baseDirectory, "model").toString
        data.saveAsObjectFile(dataFile)
        result.saveAsObjectFile(modelFile)

        new VrlshKnnQuery(baseDirectory, result, hasherMap, lookupProvider)
    }

    def time_build(data: RDD[(Long, Vector)]): Unit = {

        // Contexto de Spark
        val sc = data.sparkContext

        val dimension = data.first()._2.size

        val (hasher, hashOptions, radius) = hasherFactory.getHasherForDataset(data, dimension, desiredSize)

        var currentHasher = hasher
        val lookupProvider = new BroadcastLookupProvider(data)
        var currentIndices = data.map { case (id, _) => id }
        /*----- .cache()*/
        var currentRadius = radius
        var iteration = 0

        val maxIterations = 100

        // Resultado
        var result: RDD[(Double, Hash, List[Long])] = sc.emptyRDD
        val hasherMap = new KnnMetadata()

        // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
        val M = max
        val m = min
        var bucketCondition = (size: Int) => size >= m && size <= M

        var count = currentIndices.count()

        while (count > 0) {
            // Se guarda currentRadius porque se modifica al final del while y produce si se utiliza dentro de
            // operaciones en RDDs
            val savedRadius = currentRadius

            val savedHasher = currentHasher

            val currentData = currentIndices
                .flatMap(id => savedHasher.hash(lookupProvider.lookup(id), savedRadius).map(hash => (hash, id)))
            /*----- .cache()*/

            if (forAll(currentData) { case (hash, _) => isBaseHashPoint(hash) }) {
                ////////////////////////////////////////////////////////////////////////////////////////////////////
                // En la iteración actual TODOS forman buckets
                // -----> bucketCondition = _ => true
                ////////////////////////////////////////////////////////////////////////////////////////////////////
            }

            ////////////////////////////////////////////////////////////////////////////////////////////////////////
            var isLast = false
            if (count < min) {
                bucketCondition = _ => true
                isLast = true
            } else if (iteration > maxIterations) {
                bucketCondition = _ => true
                isLast = true
            }
            ////////////////////////////////////////////////////////////////////////////////////////////////////////

            val savedBucketCondition = bucketCondition

            // Se calculan los buckets (hash) y el número de puntos en cada uno
            val hashWithNumPoints = currentData
                .aggregateByKey(0)(
                    { case (numPoints, _) => numPoints + 1 },
                    (numPoints1, numPoints2) => numPoints1 + numPoints2)
                .cache()

            // Se calculan los datos que se van a usar para los buckets
            val dataForBuckets = currentData
                .subtractByKey(
                    hashWithNumPoints.filter { case (_, numPoints) => !savedBucketCondition(numPoints) }
                )
            /*----- .cache()*/

            if (!dataForBuckets.isEmpty()) {
                // Se actualiza el resultado
                result = result.union(dataForBuckets
                    .aggregateByKey(List[Long]())(
                        { case (list, id) => id :: list },
                        { case (list1, list2) => list1 ++ list2 }
                    )
                    .map { case (hash, ids) => (savedRadius, hash, ids) })
                hasherMap.put(savedRadius, savedHasher)

                // Se calculan los nuevos índices
                val usedIndices = dataForBuckets
                    .map { case (_, id) => id }
                currentIndices = currentIndices.subtract(usedIndices)
                    .cache()

                count = currentIndices.count()
            }

            // Se incrementa el radio
            currentRadius = currentRadius * radiusMultiplier

            // Se crea un nuevo hasher
            currentHasher = hashOptions.newHasher()

            iteration = iteration + 1
        }
    }

    private def showStatistics(hashWithNumPoints: RDD[(Hash, Int)], bucketCondition: Int => Boolean): Unit = {
        println("    Estadísticas: Número de puntos - Número de buckets")
        time("    Se actualizan las estadísticas") {
            hashWithNumPoints
                .filter { case (_, numPoints) => bucketCondition(numPoints) }
                .map { case (_, numPoints) => (numPoints, 1) }
                .reduceByKey((count1, count2) => count1 + count2)
                .sortByKey()
                .foreach { case (numPoints, count) => println(s"      $numPoints - $count") }
        }
    }

    private def showBucketCount(hashWithNumPoints: RDD[(Hash, Int)], bucketCondition: Int => Boolean): Unit = {
        val count = hashWithNumPoints
            .filter { case (_, numPoints) => bucketCondition(numPoints) }
            .count()
        println(s"      Total buckets: $count")
    }

    private def showEnvelopes(dataForBuckets: RDD[(Hash, Long)], lookupProvider: BroadcastLookupProvider): Unit = {
        // Se calculan el tamaño de los buckets
        dataForBuckets
            .aggregateByKey(EnvelopeDouble.EMPTY)(
                (envelope, id) => EnvelopeDouble.seqOp(envelope, lookupProvider.lookup(id)),
                EnvelopeDouble.combOp)
            .map { case (hash, envelope) => (hash, envelope.sizes.max) } // Max of the sizes of the envelope
            .foreach { case (hash, value) => println(s"    $hash - $value") }
    }

    private def showMinMaxEnvelope(dataForBuckets: RDD[(Hash, Long)], lookupProvider: BroadcastLookupProvider): Unit = {
        // Se calculan el tamaño de los buckets
        val maxEnvelopes = dataForBuckets
            .aggregateByKey(EnvelopeDouble.EMPTY)(
                (envelope, id) => EnvelopeDouble.seqOp(envelope, lookupProvider.lookup(id)),
                EnvelopeDouble.combOp)
            .map { case (_, envelope) => (envelope.sizes.min, envelope.sizes.max) } // (min, max) of the sizes of the envelope
        /*----- .cache()*/

        if (!maxEnvelopes.isEmpty()) {
            val min = maxEnvelopes.map(_._1).min()
            val max = maxEnvelopes.map(_._2).max()
            println(s"    Envelopes Min: $min Max: $max")
        }
    }
}

object VrlshKnnConstructionAlgorithm {
    def createAndStore(data: RDD[(Long, Vector)],
                       hasherFactory: HasherFactory,
                       desiredSize: Int,
                       baseDirectory: Path): VrlshKnnQuery = {
        val knnQuery = new VrlshKnnConstructionAlgorithm(hasherFactory, desiredSize, baseDirectory.toString).build(data)

        println("==== Almacena el modelo =====")

        // Almacena el modelo
        DataStore.kstore(baseDirectory.resolve("KnnQuery.dat"), knnQuery.getSerializable())

        knnQuery
    }

    def time_createAndStore(data: RDD[(Long, Vector)],
                            hasherFactory: HasherFactory,
                            desiredSize: Int,
                            baseDirectory: Path): Unit = {
        new VrlshKnnConstructionAlgorithm(hasherFactory, desiredSize, baseDirectory.toString).time_build(data)
    }

    def load(sc: SparkContext,
             baseDirectory: Path): VrlshKnnQuery = {
        val knnQuery = DataStore.kload(
            baseDirectory.resolve("KnnQuery.dat"),
            classOf[VrlshKnnQuerySerializator]).get(sc)
        knnQuery
    }
}

class VrlshKnnQuery(val baseDirectory: String,
                    val hasherMap: KnnMetadata,
                    val lookupProvider: BroadcastLookupProvider)
    extends KnnQuery {

    var radiuses: Seq[Double] = hasherMap.radiuses
    var mapRadiusHashToPoints: scala.collection.Map[Double, Map[Hash, Array[Long]]] = Map()

    def find(radius: Double, hash: Hash): Array[Long] = {
        mapRadiusHashToPoints.get(radius) match {
            case Some(map) => map.getOrElse(hash, Array[Long]())
            case None => Array[Long]()
        }
    }

    def this(baseDirectory: String,
             result: RDD[(Double, Hash, List[Long])],
             hasherMap: KnnMetadata,
             lookupProvider: BroadcastLookupProvider) = {
        this(baseDirectory, hasherMap, lookupProvider)

        mapRadiusHashToPoints = result.map { case (radius, hash, points) => (radius, (hash, points.toArray)) }
            .groupByKey()
            .map { case (radius, it) => (radius, it.toMap) }
            .collectAsMap()

        radiuses = mapRadiusHashToPoints
            .keySet
            .toList
            .sorted
    }

    def query(query: Vector,
              k: Int,
              distanceEvaluator: KnnDistance,
              statistics: StatisticsCollector): Iterable[(Double, Long)] = {
        val lookupProvider = this.lookupProvider

        var knnResult = new KnnResult()

        // Radios en los que se han encontrado puntos (son parte de la solución)
        var radiusesInResult: List[Double] = List()

        // Número de niveles recorridos
        var numLevels = 0

        val breakableLoop = new Breaks()
        breakableLoop.breakable {
            for (radius <- this.radiuses) {
                // Try next level
                numLevels = numLevels + 1

                // Evaluate remaining points
                val kremaining = k - knnResult.size
                val knnPartialResult = hasherMap
                    .getHasher(radius).get
                    .hash(query, radius)
                    .map(hash => find(radius, hash).map(id => (distanceEvaluator.distance(query, lookupProvider.lookup(id)), id)))
                    .aggregate(new KnnResult())(
                        KnnResult.seqOpOfArray(kremaining),
                        KnnResult.combOp(kremaining))

                // Combine the total solution with the partial one
                if (knnPartialResult.size > 0) {
                    radiusesInResult = radiusesInResult :+ radius
                    knnResult = KnnResult.combOp(k)(knnResult, knnPartialResult)
                }

                // Found enough points
                if (knnResult.size == k) {
                    breakableLoop.break
                }
            }
        }

        statistics.collect(
            new EvaluationStatistics(
                knnResult.size,
                knnResult.comparisons,
                knnResult.buckets,
                numLevels,
                radiusesInResult))

        knnResult.sorted
    }

    def getSerializable(): KnnQuerySerializable = new VrlshKnnQuerySerializator(this)

    def getGeneralStatistics(): GeneralStatistics = {
        val statistics = new GeneralStatistics

        // Todos los hashers son iguales
        if (radiuses.nonEmpty) {
            val hasher = hasherMap.getHasher(radiuses.head).get.asInstanceOf[EuclideanHasher]

            statistics.numTables = hasher.numTables
            statistics.keyLength = hasher.keyLength
            statistics.dimension = hasher.dimension

            val totalBuckets = radiuses.map(radius => mapRadiusHashToPoints(radius).size).sum
            val totalPoints = radiuses.map(radius => mapRadiusHashToPoints(radius).map { case (hash, points) => points.size }.sum).sum
            val totalLevels = radiuses.size
            val fraction = totalPoints / lookupProvider.size.toDouble

            statistics.ratioOfPoints = fraction
            statistics.totalNumBuckets = totalBuckets
            statistics.totalNumPoints = totalPoints
            statistics.totalNumLevels = totalLevels
        }

        statistics
    }


    def printResume(): Unit = {
        // Todos los hashers son iguales
        if (radiuses.nonEmpty) {
            //val hasher = hasherMap.getHasher(radiuses.head).get.asInstanceOf[EuclideanHasher]

            /*println("radius | Num. buckets | Num. points")
            println(":- | :-")
            radiuses
                .filter(radius => (radius != radiuses.last))
                .foreach(radius => {
                    val forRadius = mapRadiusHashToPoints(radius)
                    val numBuckets = forRadius.size
                    val numPoints = forRadius.map { case (_, points) => points.length }.sum
                    //val hasher = hasherMap.getHasher(radius).get

                    println(s"$radius | $numBuckets | $numPoints")
                })*/

            val numBuckets = radiuses
                .filter(radius => (radius != radiuses.last))
                .flatMap(radius => mapRadiusHashToPoints(radius).map { case (hash, array) => hash })
                .size

            val numPoints = radiuses
                .filter(radius => (radius != radiuses.last))
                .flatMap(radius => mapRadiusHashToPoints(radius).flatMap { case (hash, array) => array })
                .size

            val numUniquePoints = radiuses
                .filter(radius => (radius != radiuses.last))
                .flatMap(radius => mapRadiusHashToPoints(radius).flatMap { case (hash, array) => array })
                .toSet
                .size
            println(s"puntos=$numPoints únicos=$numUniquePoints buckets=$numBuckets")

            val totalBuckets = radiuses.map(radius => mapRadiusHashToPoints(radius).size).sum
            val totalPoints = radiuses.map(radius => mapRadiusHashToPoints(radius).map { case (hash, points) => points.size }.sum).sum
            val totalLevels = radiuses.size
            val fraction = totalPoints / lookupProvider.size.toDouble

            println("Último nivel")

            val forRadius = mapRadiusHashToPoints(radiuses.last)
            /*println("Num. points | Count")
            println(":- | :-")
            forRadius
                .toList
                .map { case (hash, ids) => (ids.length, 1) }
                .groupBy { case (numPoints, count) => numPoints }
                .map { case (numPoints, count) => (numPoints, count.length) }
                .toList
                .sortBy { case (numPoints, count) => numPoints }
                .foreach { case (numPoints, count) => println(s"$numPoints | $count") }*/

            val numBucketsInLastLevel = forRadius
                .size

            val numPointsInLastLevel = forRadius
                .toList
                .map { case (hash, ids) => ids.length }
                .sum

            val numUniquePointsInLastLevel = forRadius
                .toList
                .flatMap { case (hash, ids) => ids }
                .toSet
                .size
            println(s"puntos=$numPointsInLastLevel únicos=$numUniquePointsInLastLevel buckets=$numBucketsInLastLevel")
        }
    }
}

class VrlshKnnQuerySerializator(var baseDirectory: String,
                                var hasherMap: KnnMetadata)
    extends KnnQuerySerializable {

    def this() = this(null, null)

    def this(query: VrlshKnnQuery) = this(query.baseDirectory, query.hasherMap)

    def get(sc: SparkContext): VrlshKnnQuery = {
        val dataFile = Paths.get(baseDirectory, "data").toString
        val modelFile = Paths.get(baseDirectory, "model").toString

        val data = sc.objectFile[(Long, Vector)](dataFile)
        val result = sc.objectFile[(Double, Hash, List[Long])](modelFile)

        val lookupProvider = new BroadcastLookupProvider(data)

        new VrlshKnnQuery(baseDirectory, result, hasherMap, lookupProvider)
    }
}
