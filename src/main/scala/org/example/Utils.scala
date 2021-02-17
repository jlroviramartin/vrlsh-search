package org.example

object Utils {
    def log(a: Double, base: Double): Double = Math.log(a) / Math.log(base);

    def log2(n: Double): Double = Math.log10(n) / Math.log10(2);

    def square(x: Double): Double = x * x;

    /*def intersect(trEnvelope: EnvelopeDoubleBuffer, resolution: Double) = {
        trEnvelope.indices.filter(i => {
            val min = trEnvelope.min(i);
            val max = trEnvelope.max(i);
            val imin = (min * resolution).toInt;
            val imax = (max * resolution).toInt;
            if (imin != imax) {

            } else {
                
            }
        })
    }*/
}
