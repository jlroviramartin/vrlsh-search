package org.example.buckets

import org.apache.spark.mllib.linalg.Vector
import org.example.{EnvelopeDoubleBuffer, HashOptions}

class TrBucket(val trEnvelope: EnvelopeDoubleBuffer,
               envelope: EnvelopeDoubleBuffer)
    extends Bucket(envelope) {

    def this(alfa: Int, dim: Int) = this(trEnvelope = new EnvelopeDoubleBuffer(alfa), envelope = new EnvelopeDoubleBuffer(dim));

    def this(options: HashOptions) = this(alfa = options.alfa, dim = options.dim);

    def this(trPoint: Seq[Double], point: Vector) = {
        this(alfa = trPoint.length, dim = point.size);
        put(trPoint, point);
    }

    def put(trPoint: Seq[Double], point: Vector): Unit = {
        super.put(point);
        trEnvelope.add(trPoint);
    }
}
