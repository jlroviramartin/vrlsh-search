package org.vrlshsearch

import org.apache.spark.ml.linalg.Vectors
import org.vrlshsearch.buckets.Bucket
import org.scalatest.funsuite.AnyFunSuite

class BucketSuite extends AnyFunSuite {

    test("Testing buckets") {
        val bucket = new Bucket(Vectors.dense(10, 10))
        bucket.put(Vectors.dense(0, 10))
        bucket.put(Vectors.dense(0, 0))

        assertResult(new EnvelopeDoubleBuffer(Array[Double](0, 0), Array[Double](10, 10)))(bucket.envelope)
        assertResult(3)(bucket.points.size)
    }

    test("Testing buckets 2") {
        /*val bucket = new Bucket(Vectors.dense(1, 2))
        bucket.put(Vectors.dense(3, 4))

        val tempDir = Files.createTempDirectory("test_" + System.currentTimeMillis()).toFile
        tempDir.deleteOnExit()

        val file = tempDir.toPath.resolve("test.data")

        bucket.store(1, new HashPoint(1, 2, 3), file)
        val other = Bucket.load(1, new HashPoint(1, 2, 3), file).head

        assertResult(bucket)(other)*/
    }
}
