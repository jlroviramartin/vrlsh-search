package org.example

import org.scalatest.funsuite.AnyFunSuite
import org.example.Distance._

class DistanceSuite extends AnyFunSuite {

    test("Distance point to point") {
        assert(squareDistance(Seq(1.0, 1.0, 1.0), Seq(1.0, 1.0, 1.0)) == 0.0);
        assert(squareDistance(Seq(1.0, 1.0, 1.0), Seq(2.0, 2.0, 2.0)) == 3.0);
        assert(squareDistance(Seq(1.0, 1.0, 1.0), Seq(3.0, 3.0, 3.0)) == 12.0);
    }

    test("Distance point to rectangle") {
        assert(squareDistance(
            Seq(1.0, 1.0, 1.0),
            new EnvelopeDoubleBuffer(Seq(1.0, 1.0, 1.0), Seq(2.0, 2.0, 2.0))) == 0.0);
        assert(squareDistance(
            Seq(0.0, 0.0, 0.0),
            new EnvelopeDoubleBuffer(Seq(1.0, 1.0, 1.0), Seq(2.0, 2.0, 2.0))) == 3.0);
        assert(squareDistance(
            Seq(1.0, 0.0, 0.0),
            new EnvelopeDoubleBuffer(Seq(1.0, 1.0, 1.0), Seq(2.0, 2.0, 2.0))) == 2.0);
        assert(squareDistance(
            Seq(1.0, 1.0, 0.0),
            new EnvelopeDoubleBuffer(Seq(1.0, 1.0, 1.0), Seq(2.0, 2.0, 2.0))) == 1.0);

        assert(squareDistance(
            Seq(1.5, 1.5, 1.5),
            new EnvelopeDoubleBuffer(Seq(1.0, 1.0, 1.0), Seq(2.0, 2.0, 2.0))) == 0.0);
    }
}
