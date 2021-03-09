package org.example.construction

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.example.{BroadcastLookupProvider, EnvelopeDouble, HashOptions, LookupProvider, Utils}
import org.example.Utils.time
import org.example.evaluators.{Hash, HashPoint, Hasher}

import scala.collection.{Iterable, mutable}

class KnnConstructionAlgorithm(val desiredSize: Int,
                               val baseDirectory: String)
    extends KnnConstruction {

    val min = desiredSize * Utils.MIN_TOLERANCE;
    val max = desiredSize * Utils.MAX_TOLERANCE;
    val radiusMultiplier = 1.4;

    override def build(data: RDD[(Long, Vector)]): Unit = {

        // Contexto de Spark
        val sc = data.sparkContext

        val dimension = data.first()._2.size;

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

        // Condición para formar un bucket. Aquellos que no la cumplan van a la siguiente ronda.
        var bucketCondition = (size: Int) => size >= min && size <= max

        while (iteration < 30 || !time("Se comprueba si está vacío") {
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
                    Utils.forAll(currentData) { case (hash, _) => Utils.isBaseHashPoint(hash) }
                }) {
                    println("    ===== Todos son BASE =====");

                    // En la iteración actual TODOS forman buckets
                    // --> bucketCondition = _ => true PARA PROBAR SE DESACTIVA
                }

                // Se calculan los buckets (hash) y el número de puntos en cada uno
                val hashWithNumPoints = currentData
                    .aggregateByKey(0)(
                        { case (numPoints, _) => numPoints + 1 },
                        (numPoints1, numPoints2) => numPoints1 + numPoints2)

                println("    Estadísticas: Número de puntos - Número de buckets");
                time("    Se actualizan las estadísticas") {
                    hashWithNumPoints
                        .filter { case (_, numPoints) => bucketCondition(numPoints) }
                        .map { case (_, numPoints) => (numPoints, 1) }
                        .reduceByKey((count1, count2) => count1 + count2)
                        .sortByKey()
                        .foreach { case (numPoints, count) => {
                            println(s"      $numPoints - $count")
                        }
                        }
                }

                val dataForBuckets = currentData
                    .subtractByKey(
                        hashWithNumPoints.filter { case (_, numPoints) => !bucketCondition(numPoints) }
                    ).cache()

                // Se calculan el tamaño de los buckets
                val zeroVal = EnvelopeDouble.EMPTY
                val seqOp = (accumulator: EnvelopeDouble, id: Long) => accumulator.join(lookupProvider.lookup(id))
                val combOp = (accumulator1: EnvelopeDouble, accumulator2: EnvelopeDouble) => accumulator1.join(accumulator2)

                /*println("    Envelopes");
                dataForBuckets
                    .aggregateByKey(zeroVal)(seqOp, combOp)
                    .map { case (hash, envelope) => (hash, envelope.sizes.max) } // Max of the sizes of the envelope
                    .foreach { case (hash, max) => {
                        println(s"    $hash - $max")
                    }
                    }*/

                val maxEvelopes = dataForBuckets
                    .aggregateByKey(zeroVal)(seqOp, combOp)
                    .map { case (_, envelope) => envelope.sizes.max } // Max of the sizes of the envelope
                    .cache()
                if (!maxEvelopes.isEmpty()) {
                    println(s"    Máximo tamaño de los envelopes=${maxEvelopes.max()}")
                }

                // Se actualiza el resultado
                result = result.union(dataForBuckets.map { case (hash, id) => (savedRadius, hash, id) })

                val usedIndices = dataForBuckets
                    .map { case (_, id) => id }

                currentIndices = currentIndices.subtract(usedIndices)

                currentRadius = currentRadius * radiusMultiplier

                currentHasher = hashOptions.newHasher()
                if (bcurrentHasher != null) bcurrentHasher.destroy()
                bcurrentHasher = sc.broadcast(currentHasher)

                iteration = iteration + 1
            }
        }

        println("Se finaliza!")

        if (bcurrentHasher != null) bcurrentHasher.destroy()
    }

    // ----------------------------------------------------------------------------------------------------
    // A partir de aquí no se utilizan

    def showStatistics(statistics: mutable.Map[Int, Int]): Unit = {
        time {
            statistics.toSeq.sortBy(_._1).foreach { case (numPoints, count) => println(s"      $numPoints - $count") }
        }
    }

    def sizesByHash(hashedData: RDD[(Int, Long, Vector)],
                    radius: Double,
                    bhasher: Broadcast[Hasher]): RDD[(Hash, Int)] = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .aggregateByKey(0)( // Se cuenta el número de puntos en cada bucket
                { case (numPts, id) => numPts + 1 },
                { case (numPts1, numPts2) => numPts1 + numPts2 })
    }

    def filterHashes(hashedData: RDD[(Int, Long, Vector)],
                     radius: Double,
                     bhasher: Broadcast[Hasher],
                     sizeFilter: Int => Boolean): Set[Hash] = {

        val set = sizesByHash(hashedData, radius, bhasher)
            .filter { case (_, size) => sizeFilter(size) }
            .map { case (hash, _) => hash }
            .collect().toSet
        set
    }

    def updateStatistics(hashedData: RDD[(Int, Long, Vector)],
                         radius: Double,
                         bhasher: Broadcast[Hasher],
                         bsetToFilter: Broadcast[Set[Hash]],
                         statistics: mutable.Map[Int, Int]): Unit = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .filter { case (hash, (_, _)) => bsetToFilter.value.contains(hash) }
            .aggregateByKey(0)(
                { case (numPoints, (id, point)) => numPoints + 1 },
                (numPoints1, numPoints2) => numPoints1 + numPoints2)
            .collect()
            .foreach { case (hash, numPoints) =>
                Utils.addOrUpdate(statistics, numPoints, 1, (prev: Int) => prev + 1)
            }
    }

    def updateStatisticsWithoutFilter(hashedData: RDD[(Int, Long, Vector)],
                                      radius: Double,
                                      bhasher: Broadcast[Hasher],
                                      statistics: mutable.Map[Int, Int]): Unit = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .aggregateByKey(0)(
                { case (numPoints, (id, point)) => numPoints + 1 },
                (numPoints1, numPoints2) => numPoints1 + numPoints2)
            .collect()
            .foreach { case (hash, numPoints) =>
                Utils.addOrUpdate(statistics, numPoints, 1, (prev: Int) => prev + 1)
            }
    }

    def updateStatisticsForBases(hashedData: RDD[(Int, Long, Vector)],
                                 radius: Double,
                                 bhasher: Broadcast[Hasher],
                                 bsetBases: Broadcast[Set[Int]],
                                 statistics: mutable.Map[Int, Int]): Unit = {

        hashedData.map { case (tableIndex, id, point) => (bhasher.value.hash(tableIndex, point, radius), (id, point)) }
            .filter { case (hash, (_, _)) => bsetBases.value.contains(HashPoint.getIndex(hash)) }
            .aggregateByKey(0)(
                { case (numPoints, (id, point)) => numPoints + 1 },
                (numPoints1, numPoints2) => numPoints1 + numPoints2)
            .collect()
            .foreach { case (hash, numPoints) =>
                Utils.addOrUpdate(statistics, numPoints, 1, (prev: Int) => prev + 1)
            }
    }

    def removeBases(data: RDD[(Int, Long, Vector)],
                    bsetBases: Broadcast[Set[Int]]): RDD[(Int, Long, Vector)] = {

        data.filter { case (numTable, id, point) => !bsetBases.value.contains(numTable) }
    }

    def getRemaining(hashedData: RDD[(Int, Long, Vector)],
                     radius: Double,
                     bhasher: Broadcast[Hasher],
                     bsetToFilter: Broadcast[Set[Hash]]): RDD[((Int, Hash), (Long, Vector))] = {

        hashedData.map { case (tableIndex, id, point) => ((tableIndex, bhasher.value.hash(tableIndex, point, radius)), (id, point)) }
            .filter { case ((tableIndex, hash), (id, point)) => !bsetToFilter.value.contains(hash) }
    }

    def findBases(data: RDD[((Int, Hash), (Long, Vector))]): Set[Int] = {
        data.map { case ((numTable, hash), (id, point)) => (numTable, hash) }
            .aggregateByKey(true)( // Se comprueba si todos son base
                { case (isBase, hash) => isBase && Utils.isBaseHashPoint(hash) },
                { case (isBase1, isBase2) => isBase1 && isBase2 })
            .filter { case (numTable, isBase) => isBase }
            .map { case (numTable, isBase) => numTable }
            .distinct()
            .collect()
            .toSet
    }
}
