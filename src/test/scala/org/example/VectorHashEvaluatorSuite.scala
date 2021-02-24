package org.example

import org.example.evaluators.EuclideanHashEvaluator
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class VectorHashEvaluatorSuite extends AnyFunSuite {
    test("Vector hash evaluator") {
        assertResult(3)(new EuclideanHashEvaluator(new Random(), 3, 5).dimension);
        assertResult(5)(new EuclideanHashEvaluator(new Random(), 3, 5).keyLength);
    }
}