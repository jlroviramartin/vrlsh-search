package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, DataStore, KnnDistance, KnnResult, Utils}
import org.example.Utils.time
import org.example.evaluators.{EuclideanHasher, Hash, HasherFactory}
import org.apache.spark.{HashPartitioner, SparkContext}
import Utils._
import org.example.statistics.{EvaluationStatistics, GeneralStatistics, StatisticsCollector}

import java.nio.file.{Path, Paths}
import scala.collection.immutable.Iterable
import scala.collection.Map
import scala.util.control.Breaks

class VrlshKnnConstructionAlgorithm(val hasherFactory: HasherFactory,
                                    val desiredSize: Int,
                                    val baseDirectory: String)
    extends KnnConstruction {

    val min: Double = desiredSize * Utils.MIN_TOLERANCE
    val max: Double = desiredSize * Utils.MAX_TOLERANCE

    val radiusMultiplier: Double = VrlshKnnConstructionAlgorithm.defaultRadiusMultiplier

    val maxIterations = VrlshKnnConstructionAlgorithm.defaultMaxIterations
    val maxCountNoUpdates: Int = VrlshKnnConstructionAlgorithm.defaultMaxCountNoUpdates
    val minLevels: Int = VrlshKnnConstructionAlgorithm.defaultMinLevels
    val minBuckets: Int = VrlshKnnConstructionAlgorithm.defaultMinBuckets

    val repartition: Int = VrlshKnnConstructionAlgorithm.defaultRepartition

    override def build(data: RDD[(Long, Vector)]): VrlshKnnQuery = {
        // Contexto de Spark
        val sc = data.sparkContext
        sc.setCheckpointDir("C:/spark/checkpoint")

        val dimension = data.first()._2.size
        val _repartition = repartition

        println("Se evalúan los hashers")

        val (hasher, hashOptions, radius) = hasherFactory.getHasherForDataset(data, dimension, desiredSize)

        var currentHasher = hasher

        val lookupProvider = new BroadcastLookupProvider(data)

        var currentIndices = data
            .map { case (id, _) => (id, 0) }
            .repartition(_repartition)

        var currentRadius = radius
        var iteration = 0

        // Resultado
        val hasherMap = new KnnMetadata()

        time({
            // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
            val M = max
            val m = min
            var bucketCondition = (size: Int) => size >= m && size <= M

            var count = currentIndices.count()
            var countNoUpdates = 0

            var isLast = false
            while (!isLast && count > 0) {
                println(s"  R$iteration: $currentRadius")
                println(s"    puntos restantes: $count")

                // Se guarda currentRadius porque se modifica al final del while y produce si se utiliza dentro de
                // operaciones en RDDs
                val savedRadius = currentRadius

                val savedHasher = currentHasher

                ////////////////////////////////////////////////////////////////////////////////////////////////////
                val currentData = currentIndices
                    .flatMap { case (id, _) => savedHasher.hash(lookupProvider.lookup(id), savedRadius).map(hash => (hash, id)) }
                    .partitionBy(new HashPartitioner(_repartition))
                    .cache()
                ////////////////////////////////////////////////////////////////////////////////////////////////////

                /*if (forAll(currentData) { case (hash, _) => isBaseHashPoint(hash) }) {
                println("    ===== Todos son BASE =====")

                ////////////////////////////////////////////////////////////////////////////////////////////////////
                // En la iteración actual TODOS forman buckets
                // -----> bucketCondition = _ => true
                ////////////////////////////////////////////////////////////////////////////////////////////////////
                }*/

                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                if (count < min) {
                    println("    ===== No hay puntos suficientes =====")
                    bucketCondition = _ => true
                    isLast = true
                } else if (countNoUpdates > maxCountNoUpdates) {
                    println("    ===== Se ha alcanzado un número muy grande de iteraciones sin modificaciones =====")
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
                val hashWithNumPointsFiltered = currentData
                    .aggregateByKey(0)(
                        { case (numPoints, _) => numPoints + 1 },
                        (numPoints1, numPoints2) => numPoints1 + numPoints2)
                    .filter { case (_, numPoints) => !savedBucketCondition(numPoints) }
                    .cache()

                // Se calculan los datos que se van a usar para los buckets
                val dataForBuckets = currentData
                    .subtractByKey(hashWithNumPointsFiltered)
                    .cache()

                if (!dataForBuckets.isEmpty()) {
                    println("    Se actualiza el resultado")

                    // Se actualiza el resultado
                    hasherMap.put(savedRadius, savedHasher)

                    ////////////////////////////////////////////////////////////////////////////////////
                    val level = dataForBuckets
                        .aggregateByKey(List[Long]())(
                            { case (list, id) => id :: list },
                            { case (list1, list2) => list1 ++ list2 }
                        )
                        .map { case (hash, ids) => (savedRadius, hash, ids.toArray) }

                    val partial = Paths.get(baseDirectory, s"$savedRadius").toString
                    level.saveAsObjectFile(partial)
                    ////////////////////////////////////////////////////////////////////////////////////

                    println("    Se calculan los nuevos índices")

                    // Se calculan los nuevos índices
                    val usedIndices = dataForBuckets
                        .map { case (_, id) => id }

                    // Se eliminan aquellos que se hayan usado un número mínimo de veces
                    if (minLevels > 0) {
                        val _minLevels: Int = minLevels
                        currentIndices = usedIndices
                            .distinct() // <-----
                            .map(id => (id, 1))
                            .union(currentIndices)
                            .reduceByKey((count1, count2) => count1 + count2)
                            .filter { case (_, count) => count < _minLevels }

                        ////////////////////////////////////////////////////////////////////////
                        /*println("    ----- (numLevels, count) -----")
                        currentIndices
                            .map { case (id, numLevels) => (numLevels, 1) }
                            .reduceByKey((count1, count2) => count1 + count2)
                            .sortByKey()
                            .coalesce(1)
                            .foreach { case (numLevels, count) => println(s"    $numLevels $count") }*/
                        ////////////////////////////////////////////////////////////////////////

                    } else {
                        val _minBuckets: Int = minBuckets
                        currentIndices = usedIndices
                            .map(id => (id, 1))
                            .union(currentIndices)
                            .reduceByKey((count1, count2) => count1 + count2)
                            .filter { case (_, count) => count < _minBuckets }
                    }

                    currentIndices.checkpoint()
                    currentIndices = currentIndices
                        .repartition(_repartition)
                        .cache()

                    count = currentIndices.count()

                    countNoUpdates = 0
                } else {
                    countNoUpdates = countNoUpdates + 1
                }

                // Se incrementa el radio
                currentRadius = currentRadius * radiusMultiplier

                // Se crea un nuevo hasher
                currentHasher = hashOptions.newHasher()

                iteration = iteration + 1

                println("--------------------------------------------------")
            }
        })

        println("Se finaliza!")

        //if (bcurrentHasher != null) {
        //    bcurrentHasher.destroy()
        //}

        println(s"Se almacena el resultado: $baseDirectory")

        val dataFile = Paths.get(baseDirectory, "data").toString
        data.saveAsObjectFile(dataFile)

        new VrlshKnnQuery(sc, baseDirectory, hasherMap, lookupProvider)
    }

    def time_build(data: RDD[(Long, Vector)]): Unit = {
        // Contexto de Spark
        val sc = data.sparkContext
        sc.setCheckpointDir("C:/spark/checkpoint")

        val dimension = data.first()._2.size
        val _repartition = repartition

        val (hasher, hashOptions, radius) = hasherFactory.getHasherForDataset(data, dimension, desiredSize)

        var currentHasher = hasher

        val lookupProvider = new BroadcastLookupProvider(data)

        var currentIndices = data
            .map { case (id, _) => (id, 0) }
            .repartition(_repartition)

        var currentRadius = radius
        var iteration = 0

        // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
        val M = max
        val m = min
        var bucketCondition = (size: Int) => size >= m && size <= M

        var count = currentIndices.count()
        var countNoUpdates = 0

        var isLast = false
        while (!isLast && count > 0) {

            // Se guarda currentRadius porque se modifica al final del while y produce si se utiliza dentro de
            // operaciones en RDDs
            val savedRadius = currentRadius

            val savedHasher = currentHasher

            ////////////////////////////////////////////////////////////////////////////////////////////////////
            val currentData = currentIndices
                .flatMap { case (id, _) => savedHasher.hash(lookupProvider.lookup(id), savedRadius).map(hash => (hash, id)) }
                .partitionBy(new HashPartitioner(_repartition))
                .cache()
            ////////////////////////////////////////////////////////////////////////////////////////////////////

            ////////////////////////////////////////////////////////////////////////////////////////////////////////
            if (count < min) {
                bucketCondition = _ => true
                isLast = true
            } else if (countNoUpdates > maxCountNoUpdates) {
                bucketCondition = _ => true
                isLast = true
            } else if (iteration > maxIterations) {
                bucketCondition = _ => true
                isLast = true
            }
            ////////////////////////////////////////////////////////////////////////////////////////////////////////

            val savedBucketCondition = bucketCondition

            // Se calculan los buckets (hash) y el número de puntos en cada uno
            val hashWithNumPointsFiltered = currentData
                .aggregateByKey(0)(
                    { case (numPoints, _) => numPoints + 1 },
                    (numPoints1, numPoints2) => numPoints1 + numPoints2)
                .filter { case (_, numPoints) => !savedBucketCondition(numPoints) }
                .cache()

            // Se calculan los datos que se van a usar para los buckets
            val dataForBuckets = currentData
                .subtractByKey(hashWithNumPointsFiltered)
                .cache()

            if (!dataForBuckets.isEmpty()) {
                // Se calculan los nuevos índices
                val usedIndices = dataForBuckets
                    .map { case (_, id) => id }

                // Se eliminan aquellos que se hayan usado un número mínimo de veces
                if (minLevels > 0) {
                    val _minLevels: Int = minLevels
                    currentIndices = usedIndices
                        .distinct() // <-----
                        .map(id => (id, 1))
                        .union(currentIndices)
                        .reduceByKey((count1, count2) => count1 + count2)
                        .filter { case (_, count) => count < _minLevels }
                } else {
                    val _minBuckets: Int = minBuckets
                    currentIndices = usedIndices
                        .map(id => (id, 1))
                        .union(currentIndices)
                        .reduceByKey((count1, count2) => count1 + count2)
                        .filter { case (_, count) => count < _minBuckets }
                }

                currentIndices.checkpoint()
                currentIndices = currentIndices
                    .repartition(_repartition)
                    .cache()

                count = currentIndices.count()

                countNoUpdates = 0
            } else {
                countNoUpdates = countNoUpdates + 1
            }

            // Se incrementa el radio
            currentRadius = currentRadius * radiusMultiplier

            // Se crea un nuevo hasher
            currentHasher = hashOptions.newHasher()

            iteration = iteration + 1
        }
    }
}

object VrlshKnnConstructionAlgorithm {

    var defaultRadiusMultiplier = 1.4
    var defaultMaxIterations = 200 // 100
    var defaultMaxCountNoUpdates = 50

    // Indica cual es el número mínimo de niveles en que tiene que estar un punto.
    // Solo puede estar activo defaultMinLevels o defaultMinBuckets.
    var defaultMinLevels = 0

    // Indica cual es el número mínimo de buckets en que tiene que estar un punto
    // Solo puede estar activo defaultMinLevels o defaultMinBuckets.
    var defaultMinBuckets = 1

    var defaultRepartition = 20

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

    def load(sc: SparkContext, baseDirectory: Path): VrlshKnnQuery = {
        val knnQuery = DataStore.kload(
            baseDirectory.resolve("KnnQuery.dat"),
            classOf[VrlshKnnQuerySerializator]).get(baseDirectory.toString, sc)
        knnQuery
    }
}

class VrlshKnnQuery(val hasherMap: KnnMetadata,
                    val lookupProvider: BroadcastLookupProvider)
    extends KnnQuery {

    var radiuses: Seq[Double] = hasherMap.radiuses
    var mapRadiusHashToPoints: Map[Double, Map[Hash, Array[Long]]] = Map()

    def find(radius: Double, hash: Hash): Array[Long] = {
        mapRadiusHashToPoints.get(radius) match {
            case Some(map) => map.getOrElse(hash, Array[Long]())
            case None => Array[Long]()
        }
    }

    def this(sc: SparkContext,
             baseDirectory: String,
             hasherMap: KnnMetadata,
             lookupProvider: BroadcastLookupProvider) = {
        this(hasherMap, lookupProvider)

        println("Se carga los niveles")
        mapRadiusHashToPoints = hasherMap.radiuses
            .map(
                radius => {
                    val partial = Paths.get(baseDirectory, s"$radius").toString
                    val data = sc.objectFile[(Double, Hash, Array[Long])](partial)
                        .map { case (_, hash, points) => (hash, points) }
                    (radius, data.collectAsMap())
                }
            ).toMap

        radiuses = hasherMap
            .radiuses
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

        // Número de niveles visitados
        var numLevelsVisited = 0

        val breakableLoop = new Breaks()
        breakableLoop.breakable {
            for (radius <- this.radiuses) {
                // Try next level
                numLevels = numLevels + 1

                val prevBuckets = knnResult.buckets

                val knnPartialResult = hasherMap
                    .getHasher(radius).get
                    .hash(query, radius)
                    .map(hash => find(radius, hash).map(id => (distanceEvaluator.distance(query, lookupProvider.lookup(id)), id)))
                    .aggregate(new KnnResult())(
                        KnnResult.seqOpOfArray(k),
                        KnnResult.combOp(k))
                knnResult = KnnResult.combOp(k)(knnResult, knnPartialResult)

                // Indica si se ha encontrado al menos un bucket
                val foundBucket = (knnResult.buckets - prevBuckets) > 0
                if (foundBucket) {
                    radiusesInResult = radiusesInResult :+ radius
                    numLevelsVisited = numLevelsVisited + 1
                }

                // Found enough points
                if (numLevelsVisited >= VrlshKnnQuery.searchInLevels && knnResult.size == k) {
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

object VrlshKnnQuery {
    var searchInLevels: Integer = 1;
}

class VrlshKnnQuerySerializator(var hasherMap: KnnMetadata)
    extends KnnQuerySerializable {

    def this() = this(new KnnMetadata())

    def this(query: VrlshKnnQuery) = this(query.hasherMap)

    def get(baseDirectory: String, sc: SparkContext): VrlshKnnQuery = {
        val dataFile = Paths.get(baseDirectory, "data").toString
        val modelFile = Paths.get(baseDirectory, "model").toString

        val data = sc.objectFile[(Long, Vector)](dataFile)
        val result = sc.objectFile[(Double, Hash, List[Long])](modelFile)

        val lookupProvider = new BroadcastLookupProvider(data)

        new VrlshKnnQuery(sc: SparkContext, baseDirectory, hasherMap, lookupProvider)
    }
}
