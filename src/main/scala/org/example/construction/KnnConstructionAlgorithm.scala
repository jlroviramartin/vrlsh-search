package org.example.construction

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, DataStore, EnvelopeDouble, KnnDistance, KnnResult, Utils}
import org.example.Utils.time
import org.example.evaluators.{Hash, Hasher, HasherFactory}
import org.apache.spark.SparkContext
import Utils._
import org.apache.spark.storage.StorageLevel

import java.nio.file.{Path, Paths}
import scala.collection.immutable.Iterable
import scala.reflect.io.{Directory, File}
import scala.util.control.Breaks

class KnnConstructionAlgorithm(val hasherFactory: HasherFactory,
                               val desiredSize: Int,
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
            hasherFactory.getHasherForDataset(data, dimension, desiredSize)
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        var currentHasher = hasher
        //var currentHasher = hashOptions.newHasher()
        //var bcurrentHasher = sc.broadcast(currentHasher)

        println("Inicio broadcast")

        val lookupProvider = time("Se hace broadcast de los datos") {
            new BroadcastLookupProvider(data)
        }

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
            time("Iteración") {
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
                    .persist(StorageLevel.DISK_ONLY)
                /*----- .cache()*/

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
                /*----- .cache()*/

                // Se muestran estadísticas: cuantos buckets tienen un cierto número de puntos
                //showStatistics(hashWithNumPoints, bucketCondition)
                if (isLast) {
                    showStatistics(hashWithNumPoints, bucketCondition)
                }
                showBucketCount(hashWithNumPoints, bucketCondition)

                println("Se calculan los datos que se van a usar para los buckets")

                // Se calculan los datos que se van a usar para los buckets
                val dataForBuckets = currentData
                    .subtractByKey(
                        hashWithNumPoints.filter { case (_, numPoints) => !savedBucketCondition(numPoints) }
                    ) /*----- .cache()*/

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
                        .persist(StorageLevel.DISK_ONLY)
                    /*----- .cache()*/

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

        new MyKnnQuery(baseDirectory, result, hasherMap, lookupProvider)
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
        /*----- .cache()*/

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
    /*def createOrLoad(data: RDD[(Long, Vector)],
                     desiredSize: Int,
                     baseDirectory: Path): KnnQuery = {
        val knnQuery: KnnQuery =
            if (!new File(baseDirectory.resolve("KnnQuery.dat").toFile).exists) {
                createAndStore(data, desiredSize, baseDirectory)
            }
            else {
                load(data.sparkContext, baseDirectory)
            }
        knnQuery
    }*/

    def createAndStore(data: RDD[(Long, Vector)],
                       hasherFactory: HasherFactory,
                       desiredSize: Int,
                       baseDirectory: Path): KnnQuery = {
        val knnQuery = new KnnConstructionAlgorithm(hasherFactory, desiredSize, baseDirectory.toString).build(data)

        println("==== Alacena el modelo =====")

        // Almacena el modelo
        DataStore.kstore(baseDirectory.resolve("KnnQuery.dat"), knnQuery.getSerializable())

        knnQuery
    }

    def load(sc: SparkContext,
             baseDirectory: Path): KnnQuery = {
        val knnQuery = DataStore.kload(
            baseDirectory.resolve("KnnQuery.dat"),
            classOf[MyKnnQuerySerializator]).get(sc)
        knnQuery
    }
}

class MyKnnQuery(val baseDirectory: String,
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
            new MyStatisticRow(knnResult.size,
                knnResult.comparisons,
                knnResult.buckets,
                numLevels,
                radiusesInResult))

        knnResult.sorted
    }

    def getSerializable(): KnnQuerySerializable = new MyKnnQuerySerializator(this)

    def printResume(): Unit = {
        // Todos los hashers son iguales
        if (radiuses.nonEmpty) {
            val hasher = hasherMap.getHasher(radiuses.head).get
            println(s"**hasher**: $hasher")
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

            val totalBuckets = radiuses.map(radius => mapRadiusHashToPoints(radius).size).sum
            val totalPoints = radiuses.map(radius => mapRadiusHashToPoints(radius).map { case (hash, points) => points.size }.sum).sum
            val totalLevels = radiuses.size
            val percentage = 100 * totalPoints / lookupProvider.size.toDouble

            println(s"Total num. buckets = $totalBuckets")
            println()

            println(s"Total num. points = $totalPoints")
            println()

            println(s"Total num. levels = $totalLevels")
            println()

            println(s"Percentage of points = $percentage")
            println()

            println("**Last level**")
            println("---")
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
                .foreach { case (numPoints, count) => println(s"$numPoints | $count") }
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
