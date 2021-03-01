package org.example

import org.apache.hadoop.yarn.util.RackResolver

import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.example.evaluators.HashPoint

object Utils {
    val MIN_TOLERANCE = 0.5 // 0.4
    val MAX_TOLERANCE = 1.5 // 1.1

    def log(a: Double, base: Double): Double = Math.log(a) / Math.log(base);

    def log2(n: Double): Double = Math.log10(n) / Math.log10(2);

    def square(x: Double): Double = x * x;

    def equalsWithTolerance(value: Int, desired: Int): Boolean =
        (value > MIN_TOLERANCE * desired) &&
            (value < MAX_TOLERANCE * desired);

    def equalsWithTolerance[T](values: Iterable[T], desired: Int): Boolean = {
        var count = 0
        values.takeWhile(_ => {
            count += 1
            (count >= MAX_TOLERANCE * desired)
        })

        (count > MIN_TOLERANCE * desired) &&
            (count < MAX_TOLERANCE * desired);
    }

    def equalsWithTolerance[T](values: Iterable[T], min: Int, max: Int): Boolean = {
        var count = 0
        values.takeWhile(_ => {
            count += 1
            (count >= max)
        })

        (count > min) &&
            (count < max);
    }

    /**
     * Todas los valores excepto el último (representa el indice) están en [-1, 1).
     */
    def isBaseHashPoint(hash: HashPoint): Boolean = {
        (0 until hash.values.size - 1).map(i => hash.values(i)).forall(value => value >= -1 && value < 1)
    }

    // all()
    def forAll[T](rdd: RDD[T])(p: T => Boolean): Boolean = {
        rdd.aggregate(true)((b, t) => b && p(t), _ && _)
    }

    // any()
    def exist[T](rdd: RDD[T])(p: T => Boolean): Boolean = {
        rdd.aggregate(false)((b, t) => b || p(t), _ || _)
    }

    def addOrUpdate[K, V](m: collection.mutable.Map[K, V], k: K, v: V, f: V => V) {
        m.get(k) match {
            case Some(e) => m.update(k, f(e))
            case None => m += k -> v
        }
    }

    def time[R](text: String)(block: => R): R = {
        val t0 = System.nanoTime()
        val result = block // call-by-name
        val t1 = System.nanoTime()
        val difference = t1 - t0
        val hours = TimeUnit.NANOSECONDS.toHours(difference);
        val minutes = TimeUnit.NANOSECONDS.toMinutes(difference) - TimeUnit.HOURS.toMinutes(hours);
        val seconds = TimeUnit.NANOSECONDS.toSeconds(difference) - TimeUnit.HOURS.toSeconds(hours) - TimeUnit.MINUTES.toSeconds(minutes);
        val nanos = difference - TimeUnit.MINUTES.toSeconds(minutes) - TimeUnit.HOURS.toNanos(hours) - TimeUnit.MINUTES.toNanos(minutes) - TimeUnit.SECONDS.toNanos(seconds);

        println(s"${text}Elapsed time: $hours:$minutes:$seconds,$nanos")
        result
    }

    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block // call-by-name
        val t1 = System.nanoTime()
        val difference = t1 - t0
        val hours = TimeUnit.NANOSECONDS.toHours(difference);
        val minutes = TimeUnit.NANOSECONDS.toMinutes(difference) - TimeUnit.HOURS.toMinutes(hours);
        val seconds = TimeUnit.NANOSECONDS.toSeconds(difference) - TimeUnit.HOURS.toSeconds(hours) - TimeUnit.MINUTES.toSeconds(minutes);
        val nanos = difference - TimeUnit.MINUTES.toSeconds(minutes) - TimeUnit.HOURS.toNanos(hours) - TimeUnit.MINUTES.toNanos(minutes) - TimeUnit.SECONDS.toNanos(seconds);

        println(s"Elapsed time: $hours:$minutes:$seconds,$nanos")
        result
    }

    def quiet_logs() = {
        Logger.getLogger(classOf[RackResolver]).getLevel
        Logger.getLogger("org.example").setLevel(Level.ALL)
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
    }

    def withResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
        val resource: T = r
        require(resource != null, "resource is null")
        var exception: Throwable = null
        try {
            f(resource)
        } catch {
            case NonFatal(e) =>
                exception = e
                throw e
        } finally {
            closeAndAddSuppressed(exception, resource)
        }
    }

    private def closeAndAddSuppressed(e: Throwable,
                                      resource: AutoCloseable): Unit = {
        if (e != null) {
            try {
                resource.close()
            } catch {
                case NonFatal(suppressed) =>
                    e.addSuppressed(suppressed)
            }
        } else {
            resource.close()
        }
    }

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
