package org.example

import org.example.buckets.TrBuckets
import org.example.evaluators.{DefaultHasher, EuclideanHashEvaluator, HashEvaluator, Hasher, TransformHashEvaluator}

import scala.util.Random

class HashOptions(val random: Random = new Random(),
                  val dim: Int, // Dimensión de los puntos
                  val keyLength: Int, // Longitud de la clave/Dimensión de proyección (alfa = keyLength)
                  val numTables: Int) // Número de tablas (beta = numTables)
    extends Serializable {

    def newHasher(): Hasher = new DefaultHasher(this);

    def newHashEvaluator(): HashEvaluator = new EuclideanHashEvaluator(this);

    def newTrEvaluator(): TransformHashEvaluator = {
        new EuclideanHashEvaluator(this);
    }

    def newTrBuckets(): TrBuckets = {
        new TrBuckets(this);
    }
}
