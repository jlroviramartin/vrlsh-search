package org.example

import org.apache.spark.ml.linalg.Vectors
import org.example.buckets.Bucket
import org.example.evaluators.HashPoint
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

class UtilsSuite extends AnyFunSuite {

    test("Testing isBaseHashPoint") {
        assert(!Utils.isBaseHashPoint(new HashPoint(0, 1, 2, 3)))
        assert(!Utils.isBaseHashPoint(new HashPoint(0, 0, 2, 3)))
        assert(!Utils.isBaseHashPoint(new HashPoint(0, -1, -2, 3)))

        assert(Utils.isBaseHashPoint(new HashPoint(0, 0, 0, 3)))
        assert(Utils.isBaseHashPoint(new HashPoint(0, 0, 0, 0)))
        assert(Utils.isBaseHashPoint(new HashPoint(-1, -1, -1, 3)))
        assert(Utils.isBaseHashPoint(new HashPoint(-1, -1, -1, 0)))
        assert(Utils.isBaseHashPoint(new HashPoint(0, 0, -1, 0)))
    }

    test("Testing addOrUpdate") {
        val statistics = collection.mutable.Map[Int, Int]();
        Utils.addOrUpdate(statistics, 1, 1, (v: Int) => v + 1)
        Utils.addOrUpdate(statistics, 2, 2, (v: Int) => v + 2)
        Utils.addOrUpdate(statistics, 3, 3, (v: Int) => v + 3)

        Utils.addOrUpdate(statistics, 1, 3, (v: Int) => v + 3)
        Utils.addOrUpdate(statistics, 2, 2, (v: Int) => v + 2)
        Utils.addOrUpdate(statistics, 3, 1, (v: Int) => v + 1)

        assertResult(4)(statistics(1))
        assertResult(4)(statistics(2))
        assertResult(4)(statistics(3))
    }
}
