package org.example

import org.apache.spark.ml.linalg.Vectors
import org.example.buckets.Bucket
import org.example.evaluators.{DefaultHasher, EuclideanHashEvaluator, HashPoint, LineEvaluator}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class DataStoreSuite extends AnyFunSuite {
    test("Testing store/load") {
        val bucket = new Bucket(Vectors.dense(1, 2))
        bucket.put(Vectors.dense(3, 4))

        val tempDir = Files.createTempDirectory("test_" + System.currentTimeMillis()).toFile
        tempDir.deleteOnExit()

        val file = tempDir.toPath.resolve("test.data")
        file.toFile.deleteOnExit()

        val source = new HashPoint(1, 2, 3)
        DataStore.store(file, source)
        val ret = DataStore.load[HashPoint](file)

        assertResult(source)(ret)
    }

    test("Testing kstore/kload") {
        val bucket = new Bucket(Vectors.dense(1, 2))
        bucket.put(Vectors.dense(3, 4))

        val tempDir = Files.createTempDirectory("test_" + System.currentTimeMillis()).toFile
        tempDir.deleteOnExit()

        val file = tempDir.toPath.resolve("test.data")
        file.toFile.deleteOnExit()

        val source = new HashPoint(1, 2, 3)

        DataStore.kstore(file, source)
        val ret = DataStore.kload(file, classOf[HashPoint])

        assertResult(source)(ret)
    }

    test("Testing kstore/kload (2)") {
        val bucket = new Bucket(Vectors.dense(1, 2))
        bucket.put(Vectors.dense(3, 4))

        val tempDir = Files.createTempDirectory("test_" + System.currentTimeMillis()).toFile
        tempDir.deleteOnExit()

        val file2 = tempDir.toPath.resolve("test2.data")
        file2.toFile.deleteOnExit()

        val source2 = (0.0, new DefaultHasher(
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0))),
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0)))))

        DataStore.kstore(file2, source2)
        val ret2 = DataStore.kload(file2, classOf[(Double, DefaultHasher)])

        assertResult(source2)(ret2)
    }
}
