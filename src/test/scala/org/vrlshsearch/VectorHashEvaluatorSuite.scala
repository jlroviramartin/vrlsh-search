package org.vrlshsearch

import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

import org.vrlshsearch.evaluators.EuclideanHashEvaluator
import org.vrlshsearch.Utils.RANDOM_SEED

class VectorHashEvaluatorSuite extends AnyFunSuite {
    test("Vector hash evaluator") {
        assertResult(3)(new EuclideanHashEvaluator(new Random(RANDOM_SEED), 3, 5).dimension)
        assertResult(5)(new EuclideanHashEvaluator(new Random(RANDOM_SEED), 3, 5).keyLength)
    }
}