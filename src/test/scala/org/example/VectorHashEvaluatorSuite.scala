package org.example

import org.example.evaluators.EuclideanHashEvaluator
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class VectorHashEvaluatorSuite extends AnyFunSuite {
    test("Vector hash evaluator") {
        assert(new EuclideanHashEvaluator(new Random(), 3, 5).dimension == 3);
        assert(new EuclideanHashEvaluator(new Random(), 3, 5).keyLength == 5);
    }
}