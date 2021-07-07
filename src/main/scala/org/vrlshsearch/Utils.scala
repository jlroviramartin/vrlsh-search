package org.vrlshsearch

import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal
import scala.util.Random
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{FileAppender, Level, Logger, PatternLayout}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.vrlshsearch.evaluators.Hash
import org.vrlshsearch.evaluators.HashPoint

import java.nio.file.{Files, Path, Paths}
import scala.reflect.io.Directory

object Utils {
    val MIN_TOLERANCE = 0.4 // 0.5
    val MAX_TOLERANCE = 1.1 // 1.5

    val RANDOM_SEED = 0
    val RANDOM = new Random(RANDOM_SEED)

    def log(a: Double, base: Double): Double = Math.log(a) / Math.log(base)

    def log2(n: Double): Double = Math.log10(n) / Math.log10(2)

    def square(x: Double): Double = x * x

    def equalsWithTolerance(value: Int, desired: Int): Boolean =
        (value > MIN_TOLERANCE * desired) &&
            (value < MAX_TOLERANCE * desired)

    def equalsWithTolerance[T](values: Iterable[T], desired: Int): Boolean = {
        var count = 0
        values.takeWhile(_ => {
            count += 1
            return count >= MAX_TOLERANCE * desired
        })

        (count > MIN_TOLERANCE * desired) &&
            (count < MAX_TOLERANCE * desired)
    }

    def equalsWithTolerance[T](values: Iterable[T], min: Int, max: Int): Boolean = {
        var count = 0
        values.takeWhile(_ => {
            count += 1
            return count >= max
        })

        (count > min) &&
            (count < max)
    }

    /**
     * Todas los valores excepto el último (representa el indice) están en [-1, 1).
     */
    def isBaseHashPoint(hash: Hash): Boolean = {
        HashPoint.get(hash).values.forall(value => value >= -1 && value < 1)
        //(0 until hash.values.size - 1).map(i => hash.values(i)).forall(value => value >= -1 && value < 1)
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
        val hours = TimeUnit.NANOSECONDS.toHours(difference)
        val minutes = TimeUnit.NANOSECONDS.toMinutes(difference) - TimeUnit.HOURS.toMinutes(hours)
        val seconds = TimeUnit.NANOSECONDS.toSeconds(difference) - TimeUnit.HOURS.toSeconds(hours) - TimeUnit.MINUTES.toSeconds(minutes)
        val nanos = difference - TimeUnit.MINUTES.toSeconds(minutes) - TimeUnit.HOURS.toNanos(hours) - TimeUnit.MINUTES.toNanos(minutes) - TimeUnit.SECONDS.toNanos(seconds)

        println(s"$text - Elapsed time: $hours:$minutes:$seconds,$nanos")
        result
    }

    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block // call-by-name
        val t1 = System.nanoTime()
        val difference = t1 - t0
        val hours = TimeUnit.NANOSECONDS.toHours(difference)
        val minutes = TimeUnit.NANOSECONDS.toMinutes(difference) - TimeUnit.HOURS.toMinutes(hours)
        val seconds = TimeUnit.NANOSECONDS.toSeconds(difference) - TimeUnit.HOURS.toSeconds(hours) - TimeUnit.MINUTES.toSeconds(minutes)
        val nanos = difference - TimeUnit.MINUTES.toSeconds(minutes) - TimeUnit.HOURS.toNanos(hours) - TimeUnit.MINUTES.toNanos(minutes) - TimeUnit.SECONDS.toNanos(seconds)

        println(s"Elapsed time: $hours:$minutes:$seconds,$nanos")
        result
    }

    def quiet_logs(): Unit = {
        //Logger.getLogger(classOf[RackResolver]).getLevel

        //val fa = new FileAppender()
        //fa.setName("FileLogger")
        //fa.setFile("C:/Temp/mylog.log")
        //fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"))
        //fa.setThreshold(Level.DEBUG)
        //fa.setAppend(true)
        //Logger.getRootLogger.addAppender(fa)

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

    // https://adaickalavan.github.io/scala/merging-two-sorted-lists-in-scala/
    def merge[T](i: List[T], j: List[T], compare: (T, T) => Int): List[T] = {
        (i, j) match {
            case (Nil, Nil) => Nil
            case (x :: xs, Nil) => i
            case (Nil, y :: ys) => j
            case (x :: xs, y :: ys) => {
                if (compare(x, y) <= 0)
                    x :: merge(i.tail, j, compare)
                else
                    y :: merge(i, j.tail, compare)
            }
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

    def splitDataByFilename(sc: SparkContext, file: String, trainPercentage: Int, seed: Long = RANDOM_SEED): Unit = {
        splitData(sc, Paths.get(file), trainPercentage, seed)
    }

    def splitData(sc: SparkContext, file: Path, trainPercentage: Int, seed: Long = RANDOM_SEED): Unit = {
        val testPercentage = 100 - trainPercentage
        val result = sc
            .textFile(file.toString)
            .randomSplit(Array(trainPercentage, testPercentage), seed)
        var name = file.getFileName.toString
        if (name.endsWith(".csv")) {
            name = name.substring(0, name.length - ".csv".length)
        }

        val dir1 = file.getParent.resolve(name + s"_$trainPercentage")
        val dir2 = file.getParent.resolve(name + s"_$testPercentage")
        result(0)
            .repartition(1)
            .saveAsTextFile(dir1.toString)
        result(1)
            .repartition(1)
            .saveAsTextFile(dir2.toString)


        val file1 = dir1.getParent.resolve(dir1.getFileName.toString + ".csv")
        val file2 = dir2.getParent.resolve(dir2.getFileName.toString + ".csv")
        Files.deleteIfExists(file1)
        Files.deleteIfExists(file2)

        Files.move(dir1.resolve("part-00000"), file1)
        Files.move(dir2.resolve("part-00000"), file2)

        new Directory(dir1.toFile).deleteRecursively()
        new Directory(dir2.toFile).deleteRecursively()
    }
}
