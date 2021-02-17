package org.example

import org.apache.spark.ml.linalg.Vectors
import org.example.evaluators.LineEvaluator
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class LineEvaluatorSuite extends AnyFunSuite {
    test("Line evaluator") {
        assert(new LineEvaluator(new Random(), 3).dimension == 3);
        assert(new LineEvaluator(Seq(1.0, 2.0), 3.0).evaluate(Vectors.dense(1.0, 1.0)) == 6.0);
        assert(new LineEvaluator(Seq(2.0, 3.0), 3.0).evaluate(Vectors.dense(4.0, 5.0)) == 26.0);
        assert(new LineEvaluator(Seq(1.0, 2.0), 3.0).evaluate(Vectors.dense(1.0, 1.0), 2.0) == 12.0);
        assert(new LineEvaluator(Seq(2.0, 3.0), 3.0).evaluate(Vectors.dense(4.0, 5.0), 2.0) == 52.0);
    }
}
