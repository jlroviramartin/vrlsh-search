package org.example.evaluators

import org.example.HashOptions

import scala.util.Random

class KeyHashEvaluator(evaluators: Seq[HashEvaluator]) {
    def this(random: Random, dim: Int, alfa: Int, beta: Int) {
        this((0 until beta).map(_ => new VectorHashEvaluator(random, dim, alfa)));
    }

    def this(options: HashOptions) {
        this((0 until options.beta).map(_ => options.newHashEvaluator()));
    }

    def hash(v: Seq[Double], resolution: Double): Seq[HashPoint] = {
        evaluators.map(evaluator => evaluator.hash(v, resolution))
    }
}
