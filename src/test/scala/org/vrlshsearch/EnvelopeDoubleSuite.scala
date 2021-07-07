package org.vrlshsearch

import org.scalatest.funsuite.AnyFunSuite

class EnvelopeDoubleSuite extends AnyFunSuite {

    test("Testing join") {
        var envelope = EnvelopeDouble.EMPTY
        envelope = envelope.join(new EnvelopeDouble(10, 20, 30))
        envelope = envelope.join(new EnvelopeDouble(20, 60, 10))
        envelope = envelope.join(new EnvelopeDouble(-5, 0, 100))
        assertResult(
            new EnvelopeDouble(
                Array(-5.0, 0.0, 10.0),
                Array(20.0, 60.0, 100.0)))(
            envelope)

        var envelope2 = EnvelopeDouble.EMPTY
        envelope2 = envelope2.join(new EnvelopeDouble(10, 20, 30))
        assertResult(
            new EnvelopeDouble(
                Array(10.0, 20.0, 30.0),
                Array(10.0, 20.0, 30.0))
        )(
            envelope2)

        val envelope3 = EnvelopeDouble.EMPTY
        assertResult(
            EnvelopeDouble.EMPTY
        )(
            envelope3)
    }

    test("Testing size") {
        var envelope = EnvelopeDouble.EMPTY
        envelope = envelope.join(new EnvelopeDouble(10, 20, 30))
        envelope = envelope.join(new EnvelopeDouble(20, 60, 10))
        envelope = envelope.join(new EnvelopeDouble(-5, 0, 100))
        assertResult(
            Array(25.0, 60.0, 90.0).toSeq)(
            envelope.sizes)

        var envelope2 = EnvelopeDouble.EMPTY
        envelope2 = envelope2.join(new EnvelopeDouble(10, 20, 30))
        assertResult(
            Array(0.0, 0.0, 0.0).toSeq
        )(
            envelope2.sizes)

        val envelope3 = EnvelopeDouble.EMPTY
        assertResult(
            Array().toSeq
        )(
            envelope3.sizes)
    }

    test("Testing size max") {
        var envelope = EnvelopeDouble.EMPTY
        envelope = envelope.join(new EnvelopeDouble(10, 20, 30))
        envelope = envelope.join(new EnvelopeDouble(20, 60, 10))
        envelope = envelope.join(new EnvelopeDouble(-5, 0, 100))
        assertResult(
            90.0)(
            envelope.sizes.max)

        var envelope2 = EnvelopeDouble.EMPTY
        envelope2 = envelope2.join(new EnvelopeDouble(10, 20, 30))
        assertResult(
            0.0
        )(
            envelope2.sizes.max)

        val envelope3 = EnvelopeDouble.EMPTY
        assertThrows[UnsupportedOperationException](
            envelope3.sizes.max)
    }
}
