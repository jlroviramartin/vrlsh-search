package org.example

import Utils._

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector

/**
 * Distance utilities.
 */
object Distance {
    /**
     * Squared distance between two points.
     *
     * @param point1 Point 1.
     * @param point2 Point 2.
     * @return Squared distance.
     */
    def squareDistance(point1: Seq[Double], point2: Seq[Double]): Double = {
        (point1 zip point2).map { case (a, b) => square(b - a) }.sum;
    }

    /**
     * Distance between two points.
     *
     * @param point1 Point 1.
     * @param point2 Point 2.
     * @return Distance.
     */
    def distance(point1: Seq[Double], point2: Seq[Double]): Double = {
        Math.sqrt(squareDistance(point1, point2));
    }

    /**
     * Squared distance between two points.
     *
     * @param point1 Point 1.
     * @param point2 Point 2.
     * @return Squared distance.
     */
    def squareDistance(point1: Vector, point2: Vector): Double = {
        Vectors.sqdist(point1, point2)
    }

    /**
     * Distance between two points.
     *
     * @param point1 Point 1.
     * @param point2 Point 2.
     * @return Distance.
     */
    def distance(point1: Vector, point2: Vector): Double = {
        Math.sqrt(squareDistance(point1, point2));
    }

    /**
     * Squared distance from point to envelope.
     *
     * @param point    Point.
     * @param envelope Envelope.
     * @return Squared distance.
     */
    def squareDistance(point: Seq[Double], envelope: EnvelopeDoubleBuffer): Double = {
        (point zip (envelope.minimum zip envelope.maximum))
            .map { case (a, (min, max)) =>
                if (a < min)
                    square(min - a); // Lesser than min
                else if (a > max)
                    square(a - max); // Greater than max
                else
                    0.0 // Between min and max: inside
            }.sum;
    }

    /**
     * Distance from point to envelope.
     *
     * @param point    Point.
     * @param envelope Envelope.
     * @return Distance.
     */
    def distance(point: Seq[Double], envelope: EnvelopeDoubleBuffer): Double = {
        Math.sqrt(squareDistance(point, envelope));
    }

    /**
     * Squared distance from point to envelope.
     *
     * @param point    Point.
     * @param envelope Envelope.
     * @return Squared distance.
     */
    def squareDistance(point: Vector, envelope: EnvelopeDoubleBuffer): Double = {
        envelope.indices
            .map(i => {
                val a = point(i);
                val min = envelope.minimum(i);
                val max = envelope.maximum(i);
                if (a < min)
                    square(min - a); // Lesser than min
                else if (a > max)
                    square(a - max); // Greater than max
                else
                    0.0 // Between min and max: inside
            }).sum;
    }

    /**
     * Distance from point to envelope.
     *
     * @param point    Point.
     * @param envelope Envelope.
     * @return Distance.
     */
    def distance(point: Vector, envelope: EnvelopeDoubleBuffer): Double = {
        Math.sqrt(squareDistance(point, envelope));
    }
}
