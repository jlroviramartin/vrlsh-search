package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, DataStore, EnvelopeDouble, HashOptions, KnnDistance, KnnEuclideanDistance, KnnResult, Utils}
import org.example.Utils.time
import org.example.evaluators.{Hash, Hasher}
import org.apache.spark.SparkContext
import Utils._

import java.nio.file.{Paths, Path}
import java.util.Optional
import scala.collection.immutable.Iterable
import scala.reflect.io.Directory
import scala.util.control.Breaks

class KnnConstructionAlgorithm(val desiredSize: Int,
                               val baseDirectory: String)
    extends KnnConstruction {

    val min: Double = desiredSize * Utils.MIN_TOLERANCE
    val max: Double = desiredSize * Utils.MAX_TOLERANCE
    val radiusMultiplier: Double = 1.4

    override def build(data: RDD[(Long, Vector)]): KnnQuery = {

        // Contexto de Spark
        val sc = data.sparkContext

        val dimension = data.first()._2.size

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        val (hasher, hashOptions, radius) = time(s"desiredSize = $desiredSize tolerance = ($min, $max)") {
            Hasher.getHasherForDataset(data, dimension, desiredSize)
        }
        //val radius = 0.1
        //val hashOptions = new HashOptions(dimension, 16, 14)
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        var currentHasher = hasher
        //var currentHasher = hashOptions.newHasher()
        //var bcurrentHasher = sc.broadcast(currentHasher)

        val lookupProvider = time("Se hace broadcast de los datos") {
            new BroadcastLookupProvider(data)
        }

        var currentIndices = data.map { case (id, _) => id }.cache()
        var currentRadius = radius
        var iteration = 0

        val maxIterations = 100

        // Resultado
        var result: RDD[(Double, Hash, List[Long])] = sc.emptyRDD
        val hasherMap = new KnnMetadata()

        // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
        var bucketCondition = (size: Int) => size >= min && size <= max

        while (!time("Se comprueba si está vacío") {
            currentIndices.isEmpty()
        }) {
            time("Iteración") {
                println(s"  R$iteration: $currentRadius")
                val count = time("  Se cuenta el número de indices") {
                    currentIndices.count()
                }
                println(s"  puntos restantes: $count")

                // Se guarda currentRadius porque se modifica al final del while y produce si se utiliza dentro de
                // operaciones en RDDs
                val savedRadius = currentRadius

                val savedHasher = currentHasher
                //val bsavedHasher = bcurrentHasher

                val currentData = currentIndices
                    .flatMap(id => savedHasher.hash(lookupProvider.lookup(id), savedRadius).map(hash => (hash, id)))
                    .cache()

                if (time("  Se comprueba si todos son BASE") {
                    forAll(currentData) { case (hash, _) => isBaseHashPoint(hash) }
                }) {
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
                if (isLast) {
                    showStatistics(hashWithNumPoints, bucketCondition)
                }
                showBucketCount(hashWithNumPoints, bucketCondition)

                // Se calculan los datos que se van a usar para los buckets
                val dataForBuckets = currentData
                    .subtractByKey(
                        hashWithNumPoints.filter { case (_, numPoints) => !savedBucketCondition(numPoints) }
                    ).cache()

                if (!dataForBuckets.isEmpty()) {
                    // Se muestran los envelopes
                    //println("    Envelopes");
                    //showEnvelopes(dataForBuckets, lookupProvider)

                    // Se calcula el tamaño mínimo/máximo de los envelopes
                    showMinMaxEnvelope(dataForBuckets, lookupProvider)

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
                    currentIndices = currentIndices.subtract(usedIndices).cache()
                }

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

                print("--------------------------------------------------")
            }
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

        new MyKnnQuery(baseDirectory, /*data,*/ result, hasherMap, lookupProvider /*, distanceEvaluator*/)
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
        val count = time("    Se cuenta el número de buckets creados") {
            hashWithNumPoints
                .filter { case (_, numPoints) => bucketCondition(numPoints) }
                .count()
        }
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
            .cache()

        if (!maxEnvelopes.isEmpty()) {
            val min = maxEnvelopes.map(_._1).min()
            val max = maxEnvelopes.map(_._2).max()
            println(s"    Envelopes Min: $min Max: $max")
        }
    }
}

object KnnConstructionAlgorithm {
    /**
     * Tomando como directorio base {@code baseDirectory}:
     * si el directorio no existe, crea un nuevo modelo KnnQuery y lo almacena en dicho directorio;
     * en caso contrario, lo carga.
     */
    def createOrLoad(data: RDD[(Long, Vector)],
                     desiredSize: Int,
                     baseDirectory: Path): KnnQuery = {
        val directory = new Directory(baseDirectory.toFile)
        val knnQuery: KnnQuery =
            if (!directory.exists) {
                val knnQuery = new KnnConstructionAlgorithm(desiredSize, baseDirectory.toString).build(data)

                // Alacena el modelo
                DataStore.kstore(
                    baseDirectory.resolve("KnnQuery.dat"),
                    knnQuery.getSerializable())

                knnQuery
            }
            else {
                val sc = data.sparkContext

                val knnQuery = DataStore.kload(
                    baseDirectory.resolve("KnnQuery.dat"),
                    classOf[MyKnnQuerySerializator]).get(sc)
                knnQuery
            }
        knnQuery
    }
}

class MyKnnQuery(val baseDirectory: String,
                 //val result: RDD[(Double, Hash, List[Long])],
                 val hasherMap: KnnMetadata,
                 val lookupProvider: BroadcastLookupProvider /*,
                 val distanceEvaluator: KnnDistance*/)
    extends KnnQuery {

    var radiuses: Seq[Double] = hasherMap.radiuses
    /*var mapForRadius: Map[Double, RDD[(Hash, List[Long])]] = Map()*/

    //var mapRadiusHashToPoints: scala.collection.Map[(Double, Hash), Array[Long]] = Map()
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

        //map = result.map { case (radius, hash, points) => ((radius, hash), points.toArray) }
        //    .collectAsMap()

        mapRadiusHashToPoints = result.map { case (radius, hash, points) => (radius, (hash, points.toArray)) }
            .groupByKey()
            .map { case (radius, it) => (radius, it.toMap) }
            .collectAsMap()

        //radiuses = result
        //    .map { case (radius, hash, points) => radius }
        //    .aggregate(Set[Double]())(
        //        (accumulator, b) => accumulator + b,
        //        (accumulator1, accumulator2) => accumulator1 ++ accumulator2)
        //    .toList
        //    .sorted

        radiuses = mapRadiusHashToPoints
            .keySet
            .toList
            .sorted
    }

    /*def init() = {
        this.mapForRadius = this.radiuses
            .map(r => (r, result.filter { case (radius, _, _) => r == radius }
                .map { case (_, hash, ids) => (hash, ids) }
                .cache()))
            .filter(x => x._2.count() > 0)
            .toMap
    }

    init()*/

    def query(point: Vector,
              k: Int,
              distanceEvaluator: KnnDistance,
              statistics: StatisticsCollector): Iterable[(Double, Long)] = {
        //val distanceEvaluator = this.distanceEvaluator
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
                    .hash(point, radius)
                    .map(hash => find(radius, hash).map(id => (distanceEvaluator.distance(point, lookupProvider.lookup(id)), id)))
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
            new MyStatisticRow(knnResult.size,
                knnResult.comparisons,
                knnResult.buckets,
                numLevels,
                radiusesInResult))

        /*val it = this.radiuses.toIterator
        while (it.hasNext && knnResult.size < k) {
            val radius = it.next()
            numLevels = numLevels + 1

            val queries = hasherMap.getHasher(radius) match {
                case Some(hasher) => hasher.hash(point, radius).toSet
            }

            // Se calculan los puntos que faltan
            val kremaining = k - knnResult.size
            val knnPartialResult = {
                mapForRadius(radius)
                    .filter { case (hash, _) => queries.contains(hash) }
                    .map { case (_, ids) => ids.map(id => (distanceEvaluator.distanceEvaluator(point, lookupProvider.lookup(id)), id)) }
                    .aggregate(new KnnResult())(KnnResult.seqOpOfList(kremaining), KnnResult.combOp(kremaining))
            }

            if (knnPartialResult.size > 0) {
                radiusesInResult = radiusesInResult :+ radius

                knnResult = KnnResult.combOp(k)(knnResult, knnPartialResult)
            }
        }*/

        /*val breakableLoop = new Breaks()
        breakableLoop.breakable {
            for (r <- this.radius) {
                knnResult = {
                    result
                        .filter { case (radius, hash, _) => r == radius && queries.contains((radius, hash)) }
                        .map { case (_, _, ids) => ids.map(id => (distanceEvaluator.distanceEvaluator(point, lookupProvider.lookup(id)), id)) }
                        .aggregate(knnResult)(KnnResult.seqOpOfList(k), KnnResult.combOp(k))
                }

                // Found enough points
                if (knnResult.size == k) {
                    breakableLoop.break
                    //return knnResult.sorted.toList
                }
            }
        }*/
        knnResult.sorted
    }

    /*def query2(point: Vector, k: Int): Iterable[(Double, Long)] = {

        /*val distanceEvaluator = this.distanceEvaluator
        val lookupProvider = this.lookupProvider

        val r1 = result.map { case (radius, hash, ids) => ((radius, hash), ids) }

        val r2 = this.rhashers
            .flatMap { case (radius, hasher) => hasher.hash(point, radius).map(hash => ((radius, hash), 0)) }

        r1.join(r2)
            .flatMap { case (_, (ids, _)) => ids }
            .map(id => (distanceEvaluator.distanceEvaluator(point, lookupProvider.lookup(id)), id))
            .aggregate(new KnnResult())(KnnResult.seqOp(k), KnnResult.combOp(k))
            .sorted*/

        val queries = this.radiuses
            .map(radius => (radius, hasherMap.getHasher(radius)))
            .flatMap { case (radius, Some(hasher)) => hasher.hash(point, radius).map(hash => (radius, hash)) }
            .toSet

        val distanceEvaluator = this.distanceEvaluator
        val lookupProvider = this.lookupProvider

        result
            .filter { case (radius, hash, _) => queries.contains((radius, hash)) }
            .map { case (_, _, ids) => ids.map(id => (distanceEvaluator.distanceEvaluator(point, lookupProvider.lookup(id)), id)) }
            .aggregate(new KnnResult())(KnnResult.seqOpOfList(k), KnnResult.combOp(k))
            .sorted
    }*/

    def getSerializable(): KnnQuerySerializable = new MyKnnQuerySerializator(this)

    def printResume() = {
        // Todos los hashers son iguales
        if (radiuses.nonEmpty) {
            val hasher = hasherMap.getHasher(radiuses(0)).get
            println(s"hasher: $hasher")
            println()

            println("radius | Num. buckets")
            println(":- | :-")
            radiuses.foreach(radius => {
                val forRadius = mapRadiusHashToPoints(radius)
                val numBuckets = forRadius.size
                //val hasher = hasherMap.getHasher(radius).get

                println(s"$radius | $numBuckets")
            })
            println()

            println("Last level")
            println()

            val forRadius = mapRadiusHashToPoints(radiuses.last)
            println("Num. points | Count")
            println(":- | :-")
            forRadius
                .toList
                .map { case (hash, ids) => (ids.length, 1) }
                .groupBy { case (numPoints, count) => numPoints }
                .map { case (numPoints, count) => (numPoints, count.length) }
                .toList
                .sortBy { case (numPoints, count) => numPoints }
                .foreach { case (numPoints, count) => {
                    println(s"$numPoints | $count")
                }
                }
            println()
        }
    }
}

class MyKnnQuerySerializator(var baseDirectory: String,
                             var hasherMap: KnnMetadata)
    extends KnnQuerySerializable {

    def this() = this(null, null)

    def this(query: MyKnnQuery) = this(query.baseDirectory, query.hasherMap)

    def get(sc: SparkContext): MyKnnQuery = {
        val dataFile = Paths.get(baseDirectory, "data").toString
        val modelFile = Paths.get(baseDirectory, "model").toString

        val data = sc.objectFile[(Long, Vector)](dataFile)
        val result = sc.objectFile[(Double, Hash, List[Long])](modelFile)

        val lookupProvider = new BroadcastLookupProvider(data)

        new MyKnnQuery(baseDirectory, result, hasherMap, lookupProvider)
    }
}

class MyStatisticRow(val size: Int,
                     val comparisons: Int,
                     val buckets: Int,
                     val numLevels: Int,
                     val radiuses: List[Double]) extends StatisticsCollector.Row {

    def headers(): Seq[String] = List("size", "comparisons", "buckets", "numLevels")

    def data(): Seq[Any] = List(size, comparisons, buckets, numLevels)

    override def toString: String =
        s"size $size comparisons $comparisons buckets $buckets numLevels $numLevels radiuses $radiuses"
}
