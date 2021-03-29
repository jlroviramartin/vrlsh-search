package org.example

import scala.util.Random

import org.example.evaluators.{EuclideanHashEvaluator, EuclideanHasher, HashEvaluator, Hasher}
import org.example.Utils.RANDOM

class HashOptions(val random: Random,
                  val dim: Int, // Dimensión de los puntos
                  val keyLength: Int, // Longitud de la clave/Dimensión de proyección (alfa = keyLength)
                  val numTables: Int) // Número de tablas (beta = numTables)
    extends Serializable {

    def this(dim: Int, keyLength: Int, numTables: Int) = this(RANDOM, dim, keyLength, numTables)

    def newHasher(): Hasher = new EuclideanHasher(this) //new DefaultHasher(this)

    def newHashEvaluator(): HashEvaluator = new EuclideanHashEvaluator(this)

    /*def newTrEvaluator(): TransformHashEvaluator = {
        new EuclideanHashEvaluator(this);
    }*/

    /*def newTrBuckets(): TrBuckets = {
        new TrBuckets(this);
    }*/
}
