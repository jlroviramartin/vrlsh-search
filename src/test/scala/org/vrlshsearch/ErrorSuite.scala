package org.vrlshsearch

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.immutable

class ErrorSuite extends AnyFunSuite {

    test("Testing APK") {
        val realResult: Array[Long] = Array(1, 2, 3)
        val approx: Array[Long] = Array(4, 3, 2)
        val apk = Errors.evaluateAPK(3, approx, realResult)
        assertResult(0.39)(BigDecimal(apk).setScale(2, BigDecimal.RoundingMode.HALF_UP))

        val approx2: Array[Long] = Array(3, 2, 4)
        val apk2 = Errors.evaluateAPK(3, approx2, realResult)
        assertResult(0.67)(BigDecimal(apk2).setScale(2, BigDecimal.RoundingMode.HALF_UP))

        val mapk = Errors.evaluateMAPK(immutable.Iterable(apk, apk2))
        assertResult(0.53)(BigDecimal(mapk).setScale(2, BigDecimal.RoundingMode.HALF_UP))
    }

    test("Testing APK 2") {
        val a = 1
        val b = 2
        val c = 3
        val d = 4
        val x = 5
        val y = 6

        val realResult: immutable.Iterable[Array[Long]] = immutable.Iterable(Array(a, b), Array(a), Array(x, y, b))
        val approx: immutable.Iterable[Array[Long]] = immutable.Iterable(Array(a, c, d), Array(x, b, a, b), Array(y))
        val mapk = Errors.evaluateMAPK_v2(2,
            approx,
            realResult)
        assertResult(0.33)(BigDecimal(mapk).setScale(2, BigDecimal.RoundingMode.HALF_UP))


        val realResult2: immutable.Iterable[Array[Long]] = immutable.Iterable(Array(1, 5, 7, 9), Array(2, 3), Array(2, 5, 6))
        val approx2: immutable.Iterable[Array[Long]] = immutable.Iterable(Array(5, 6, 7, 8, 9), Array(1, 2, 3), Array(2, 4, 6, 8))
        val mapk2 = Errors.evaluateMAPK_v2(3,
            approx2,
            realResult2)
        //assertResult(0.56)(BigDecimal(mapk2).setScale(2, BigDecimal.RoundingMode.HALF_UP))
    }
}
