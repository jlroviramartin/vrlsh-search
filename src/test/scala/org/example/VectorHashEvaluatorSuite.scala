package org.example

import org.example.evaluators.VectorHashEvaluator
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class VectorHashEvaluatorSuite extends AnyFunSuite {
    test("Vector hash evaluator") {
        assert(new VectorHashEvaluator(new Random(), 3, 5).dimension == 3);
        assert(new VectorHashEvaluator(new Random(), 3, 5).alfa == 5);
    }
}