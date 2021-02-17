package org.example.evaluators

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging
import org.example.{HashOptions, Utils}

import scala.util.Random

trait Hasher extends Serializable {
    def hash(v: Vector, resolution: Double): Seq[HashPoint];

    final def hashData(data: RDD[(Long, LabeledPoint)], radius: Double): RDD[(Long, HashPoint)] = {
        val t = this
        val bt = data.sparkContext.broadcast(t)

        data.flatMap({ case (index, point) => bt.value.hash(point.features, radius).map(h => (index, h)) });
    }

    final def getBucketCount(data: RDD[(Long, LabeledPoint)],
                             radius: Double): (Long, Int) = {
        val currentHashes = this.hashData(data, radius)

        val bucketCountBySize = currentHashes.map(_.swap)
            .aggregateByKey(0)(
                { case (n, index) => n + 1 },
                { case (n1, n2) => n1 + n2 })
            .map({ case (h, n) => (n, 1) })
            .reduceByKey(_ + _)
            .filter({ case (n1, x) => n1 != 1 })

        val numBuckets = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.map(_._2).sum().toLong
        val largestBucketSize = if (bucketCountBySize.isEmpty()) 0 else bucketCountBySize.map(_._1).max()
        (numBuckets, largestBucketSize)
    }
}

object Hasher extends Logging {
    val MIN_TOLERANCE = 0.4
    val MAX_TOLERANCE = 1.1
    val DEFAULT_RADIUS = 0.1

    def getSuitableRadius(data: RDD[(Long, LabeledPoint)],
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

    private def computeBestKeyLength(data: RDD[(Long, LabeledPoint)], dimension: Int, desiredSize: Int): (Hasher, Double) = {
        val FRACTION = 1.0 //0.01
        val INITIAL_RADIUS = DEFAULT_RADIUS
        val initialData = data //data.sample(false, FRACTION, 56804023).map(_.swap)

        val numElems = data.count()
        var initialKLength: Int = Math.ceil(Utils.log2(numElems / dimension)).toInt + 1
        if (initialKLength < 2) initialKLength = 2

        logDebug(s"DEBUG: numElems=$numElems dimension=$dimension initialKLength=$initialKLength")

        val minKLength = if (initialKLength > 10) (initialKLength / 2).toInt else 5
        val maxKLength = if (initialKLength > 15) (initialKLength * 1.5).toInt else 22
        val hNTables: Int = Math.floor(Math.pow(Utils.log2(dimension), 2)).toInt

        val currentData = initialData
        //val currentData=initialData.sample(false, 0.2, 34652912) //20% of the data usually does the job.

        logDebug(s"Starting hyperparameter adjusting with:\n\tL:$initialKLength\n\tN:$hNTables\n\tR:$INITIAL_RADIUS\n\tC:$desiredSize")

        var (leftLimit, rightLimit) = (minKLength, maxKLength)
        var radius = INITIAL_RADIUS
        var isRadiusAdjusted = false

        while (true) {
            val currentLength = Math.floor((leftLimit + rightLimit) / 2.0).toInt

            val tmpOptions = new HashOptions(dim = dimension, keyLength = currentLength, numTables = hNTables)
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
                        val tmpOptions2 = new HashOptions(dim = dimension, keyLength = initialKLength, numTables = hNTables);
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

        val tmpOptions3 = new HashOptions(dim = dimension, keyLength = 1, numTables = hNTables);
        val tmpHasher3 = tmpOptions3.newHasher();
        (tmpHasher3, radius) //Dummy
    }

    def getHasherForDataset(data: RDD[(Long, LabeledPoint)], dimension: Int, desiredSize: Int): (Hasher, Int, Double) = {
        val (hasher, radius) = computeBestKeyLength(data, dimension, desiredSize)

        println("R0: " + radius + " " + hasher + " desiredSize: " + desiredSize)
        (hasher, desiredSize, radius);
    }
}
