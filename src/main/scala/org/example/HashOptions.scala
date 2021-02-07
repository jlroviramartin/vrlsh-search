package org.example

import org.example.buckets.TrBuckets
import org.example.evaluators.{HashEvaluator, TrHashEvaluator, VectorHashEvaluator}

import scala.util.Random

class HashOptions(
                     val random: Random = new Random(),
                     val dim: Int,
                     val alfa: Int,
                     val beta: Int) {
    def newHashEvaluator(): HashEvaluator = new VectorHashEvaluator(this);

    def newTrEvaluator(): TrHashEvaluator = {
        new VectorHashEvaluator(this);
    }

    def newTrBuckets(): TrBuckets = {
        new TrBuckets(this);
    }
}
