package org.example

import org.apache.spark.ml.linalg.Vectors
import org.example.evaluators.LineEvaluator
import org.scalatest.Assertions
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class LineEvaluatorSuite extends AnyFunSuite {
    test("Line evaluator") {
        assertResult(3)(new LineEvaluator(new Random(), 3).dimension);
        assertResult(6.0)(new LineEvaluator(Array(1.0, 2.0), 3.0).evaluate(Vectors.dense(1.0, 1.0)));
        assertResult(26.0)(new LineEvaluator(Array(2.0, 3.0), 3.0).evaluate(Vectors.dense(4.0, 5.0)));
        assertResult(12.0)(new LineEvaluator(Array(1.0, 2.0), 3.0).evaluate(Vectors.dense(1.0, 1.0), 1 / 2.0));
        assertResult(52.0)(new LineEvaluator(Array(2.0, 3.0), 3.0).evaluate(Vectors.dense(4.0, 5.0), 1 / 2.0));
    }

    test("Constructors") {
        assertResult(Array[Double](1.0, 2.0))(new LineEvaluator(1.0, 2.0, 3.0).w);
        assertResult(3.0)(new LineEvaluator(1.0, 2.0, 3.0).b);
    }
}
