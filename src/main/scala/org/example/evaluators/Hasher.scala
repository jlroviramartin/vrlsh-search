package org.example.evaluators

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging
import org.example.{HashOptions, Utils}

import scala.util.Random

@SerialVersionUID(-4061941561292649692l)
trait Hasher extends Serializable {
    /**
     * Calcula el hash del punto {@code point} para el radio {@code radius}.
     * <br>
     * Nota: los hash están indexados según el índice de tabla.
     */
    def hash(point: Vector, radius: Double): Seq[HashPoint];

    def numTables: Int;

    def hash(tableIndex: Int, point: Vector, radius: Double): HashPoint;

    def tables: Seq[HashEvaluator]

    /**
     * (id: Long, point: Vector) -> (id: Long, hash: HashPoint)
     * <br>
     * Para cada indice+punto, calcula todos los hashes del punto y los devuelve.
     */
    final def hashData(data: RDD[(Long, Vector)], radius: Double): RDD[(Long, HashPoint)] = {
        val t = this
        val bt = data.sparkContext.broadcast(t)

        data.flatMap({ case (id, point) => bt.value.hash(point, radius).map(hash => (id, hash)) });
    }

    /**
     * (id: Long, point: Vector) -> (hash: HashPoint, (id: Long, point: Vector))
     * <br>
     * Para cada indice+punto, calcula todos los hashes del punto y los devuelve.
     */
    final def hashDataWithVectors(data: RDD[(Long, Vector)], radius: Double): RDD[(HashPoint, (Long, Vector))] = {
        val t = this
        val bt = data.sparkContext.broadcast(t)

        data.flatMap({ case (id, point) => bt.value.hash(point, radius).map(hash => (hash, (id, point))) })
    }

    /**
     * Número de puntos por hash.
     */
    final def sizesByHash(data: RDD[(Long, Vector)],
                          radius: Double): RDD[(HashPoint, Int)] = {

        this.hashData(data, radius) // (id, point) -> (id, hash)
            .map(_.swap) // (id, hash) -> (hash, id)
            .aggregateByKey(0)( // Se cuenta el número de puntos en cada bucket
                { case (numPts, id) => numPts + 1 },
                { case (numPts1, numPts2) => numPts1 + numPts2 })
    }

    /**
     * Estadísticas sobre los tamaños de los buckets.
     */
    final def sizesStatistics(data: RDD[(Long, Vector)],
                              radius: Double): RDD[(Int, Int)] = {

        this.sizesByHash(data, radius) // (id, point) -> (id, hash)
            .map({ case (hash, numPts) => (numPts, 1) })
            .reduceByKey((count1, count2) => count1 + count2) // Se cuentan los buckets con el mismo número de puntos
    }

    final def getBucketCount(data: RDD[(Long, Vector)],
                             radius: Double): (Long, Int) = {
        val bucketCountBySize = this.sizesStatistics(data, radius)
            .filter({ case (numPts, count) => numPts != 1 })

        val numBuckets = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.map(_._2).sum().toLong
        val largestBucketSize = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.map(_._1).max()
        (numBuckets, largestBucketSize)
    }
}

object Hasher extends Logging {
    val MIN_TOLERANCE = Utils.MIN_TOLERANCE
    val MAX_TOLERANCE = Utils.MAX_TOLERANCE

    val DEFAULT_RADIUS = 0.1

    def getSuitableRadius(data: RDD[(Long, Vector)],
                          hasher: Hasher,
                          minValue: Double,
                          maxValue: Option[Double],
                          desiredSize: Int): Double = {
        var leftLimit = minValue
        var rightLimit =
            if (maxValue.isDefined)
                maxValue.get
            else {
                //Find a radius that is too large
                var done = false
                var currentValue = leftLimit * 2
                while (!done) {
                    val (numBuckets, largestBucketSize) = hasher.getBucketCount(data, currentValue)
                    done = largestBucketSize > desiredSize * 2
                    println(s"Radius range updated to [$leftLimit - $currentValue] got a largest bucket of $largestBucketSize")
                    if (!done)
                        currentValue *= 2
                    if ((largestBucketSize > MIN_TOLERANCE * desiredSize) &&
                        (largestBucketSize < MAX_TOLERANCE * desiredSize)) {

                        println(s"Found suitable radius at $currentValue")
                        return currentValue
                    }
                }
                currentValue
            }
        while (true) {
            val radius = (leftLimit + rightLimit) / 2
            val (numBuckets, largestBucketSize) = hasher.getBucketCount(data, radius)
            println(s"Radius update to $radius [$leftLimit - $rightLimit] got a largest bucket of $largestBucketSize")
            if ((largestBucketSize >= MIN_TOLERANCE * desiredSize) &&
                (largestBucketSize <= MAX_TOLERANCE * desiredSize)) {

                println(s"Found suitable radius at $radius")
                return radius
            }
            if ((numBuckets == 0) || (largestBucketSize < MIN_TOLERANCE * desiredSize))
                leftLimit = radius
            else if (largestBucketSize > MIN_TOLERANCE * desiredSize) {
                rightLimit = radius
            }
            if (rightLimit - leftLimit < 0.000000001) {

                println(s"WARNING! - Had to select radius = $radius")
                return radius
            }
        }
        return 1.0 //Dummy
    }

    private def computeBestKeyLength(data: RDD[(Long, Vector)], dimension: Int, desiredSize: Int): (Hasher, Double) = {
        val FRACTION = 1.0 //0.01
        val INITIAL_RADIUS = DEFAULT_RADIUS
        val initialData = data //data.sample(false, FRACTION, 56804023).map(_.swap)

        val numElems = data.count()
        var initialKLength: Int = Math.ceil(Utils.log2(numElems / dimension)).toInt + 1
        if (initialKLength < 2) initialKLength = 2

        logDebug(s"DEBUG: numElems=$numElems dimension=$dimension initialKLength=$initialKLength")

        val minKLength = if (initialKLength > 10) (initialKLength / 2).toInt else 5
        val maxKLength = if (initialKLength > 15) (initialKLength * 1.5).toInt else 22
        val numTables: Int = Math.floor(Math.pow(Utils.log2(dimension), 2)).toInt

        val currentData = initialData
        //val currentData=initialData.sample(false, 0.2, 34652912) //20% of the data usually does the job.

        logDebug(s"Starting hyperparameter adjusting with:\n\tL:$initialKLength\n\tN:$numTables\n\tR:$INITIAL_RADIUS\n\tC:$desiredSize")

        var (leftLimit, rightLimit) = (minKLength, maxKLength)
        var radius = INITIAL_RADIUS
        var isRadiusAdjusted = false

        while (true) {
            val currentLength = Math.floor((leftLimit + rightLimit) / 2.0).toInt

            logDebug(s"-- currentLength $currentLength");

            val tmpOptions = new HashOptions(dimension, currentLength, numTables)
            val tmpHasher = tmpOptions.newHasher()

            val (numBuckets, largestBucketSizeSample) = tmpHasher.getBucketCount(currentData, radius)
            val largestBucketSize = largestBucketSizeSample ///FRACTION

            if ((largestBucketSize >= desiredSize * MIN_TOLERANCE) &&
                (largestBucketSize <= desiredSize * MAX_TOLERANCE)) {

                logDebug(s"Found suitable hyperparameters:\n\tL:${tmpOptions.keyLength}\n\tN:${tmpOptions.numTables}\n\tR:$radius")

                return (tmpHasher, radius)
            } else {
                if (largestBucketSize < desiredSize * MIN_TOLERANCE) //Buckets are too small
                {
                    if ((numBuckets == 0) || (rightLimit - 1 == currentLength)) //If we ended up with no buckets with more than one element or the size is less than the desired minimum
                    {
                        if (isRadiusAdjusted) {
                            logWarning(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpOptions.keyLength}\n\tN:${tmpOptions.numTables}\n\tR:$radius")
                            return (tmpHasher, radius)
                        }

                        //We start over with a larger the radius
                        val tmpOptions2 = new HashOptions(dimension, initialKLength, numTables);
                        val tmpHasher2 = tmpOptions2.newHasher();

                        radius = getSuitableRadius(currentData, tmpHasher2, radius, None, desiredSize)
                        isRadiusAdjusted = true
                        leftLimit = minKLength
                        rightLimit = maxKLength
                    }
                    else
                        rightLimit = currentLength
                }
                else //Buckets are too large
                {
                    if (leftLimit == currentLength) {
                        if (isRadiusAdjusted) {
                            logWarning(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpOptions.keyLength}\n\tN:${tmpOptions.numTables}\n\tR:$radius")
                            return (tmpHasher, radius)
                        }

                        //We start over with a smaller the radius
                        radius = getSuitableRadius(currentData, tmpHasher, 0.000000000001, Some(radius), desiredSize)
                        isRadiusAdjusted = true
                        leftLimit = minKLength
                        rightLimit = maxKLength
                    }
                    else
                        leftLimit = currentLength
                }

                if (rightLimit <= leftLimit) {
                    logWarning(s"WARNING! - Had to go with hyperparameters:\n\tL:${tmpOptions.keyLength}\n\tN:${tmpOptions.numTables}\n\tR:$radius")
                    return (tmpHasher, radius)
                }
            }

            logDebug(s"keyLength update to ${tmpOptions.keyLength} [$leftLimit - $rightLimit] with radius $radius because largestBucket was $largestBucketSize and wanted [${desiredSize * MIN_TOLERANCE} - ${desiredSize * MAX_TOLERANCE}]")
            println(s"keyLength update to ${tmpOptions.keyLength} [$leftLimit - $rightLimit] with radius $radius because largestBucket was $largestBucketSize and wanted [${desiredSize * MIN_TOLERANCE} - ${desiredSize * MAX_TOLERANCE}]")
        }

        val tmpOptions3 = new HashOptions(dimension, 1, numTables);
        val tmpHasher3 = tmpOptions3.newHasher();
        (tmpHasher3, radius) //Dummy
    }

    def getHasherForDataset(data: RDD[(Long, Vector)], dimension: Int, desiredSize: Int): (Hasher, Double) = {
        val (hasher, radius) = computeBestKeyLength(data, dimension, desiredSize)

        println("R0: " + radius + " " + hasher + " desiredSize: " + desiredSize)
        (hasher, radius);
    }
}
