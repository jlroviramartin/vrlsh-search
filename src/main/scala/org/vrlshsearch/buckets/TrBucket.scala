package org.vrlshsearch.buckets

import org.apache.spark.ml.linalg.Vector
import org.vrlshsearch.{EnvelopeDoubleBuffer, HashOptions}

class TrBucket(val trEnvelope: EnvelopeDoubleBuffer,
               envelope: EnvelopeDoubleBuffer)
    extends Bucket(envelope) {

    def this(alfa: Int, dim: Int) = this(trEnvelope = new EnvelopeDoubleBuffer(alfa), envelope = new EnvelopeDoubleBuffer(dim));

    def this(options: HashOptions) = this(alfa = options.keyLength, dim = options.dim);

    def this(trPoint: Vector, point: Vector) = {
        this(alfa = trPoint.size, dim = point.size);
        put(trPoint, point);
    }

    def put(trPoint: Vector, point: Vector): Unit = {
        super.put(point);
        trEnvelope.add(trPoint);
    }
}
