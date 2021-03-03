package org.example

import org.example.buckets.TrBuckets
import org.example.evaluators.{DefaultHasher, EuclideanHashEvaluator, EuclideanHasher, HashEvaluator, Hasher, TransformHashEvaluator}

import scala.util.Random

class HashOptions(val random: Random,
                  val dim: Int, // Dimensión de los puntos
                  val keyLength: Int, // Longitud de la clave/Dimensión de proyección (alfa = keyLength)
                  val numTables: Int) // Número de tablas (beta = numTables)
    extends Serializable {

    def this(dim: Int, keyLength: Int, numTables: Int) = this(new Random(0), dim, keyLength, numTables);

    def newHasher(): Hasher = new EuclideanHasher(this) //new DefaultHasher(this);

    def newHashEvaluator(): HashEvaluator = new EuclideanHashEvaluator(this);

    /*def newTrEvaluator(): TransformHashEvaluator = {
        new EuclideanHashEvaluator(this);
    }*/

    /*def newTrBuckets(): TrBuckets = {
        new TrBuckets(this);
    }*/
}
