package org.example

import org.apache.spark.ml.linalg.Vectors
import org.example.evaluators.{EuclideanHashEvaluator, HashPoint, LineEvaluator}
import org.scalatest.funsuite.AnyFunSuite

class EuclideanHashEvaluatorSuite extends AnyFunSuite {

    test("Line evaluator") {
        assertResult(new HashPoint(6, 8))(
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0)))
                .hash(Vectors.dense(1.0, 1.0), 1)
        )

        assertResult(new HashPoint(12, 16))(
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0)))
                .hash(Vectors.dense(1.0, 1.0), 1.0 / 2.0)
        )

        assertResult(new HashPoint(18, 26))(
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0)))
                .hash(Vectors.dense(2.0, 2.0), 1.0 / 2.0)
        )
    }
}
