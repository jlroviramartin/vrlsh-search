package org.example

import org.apache.spark.ml.linalg.Vectors
import org.example.buckets.Bucket
import org.example.evaluators.HashPoint
import org.scalatest.funsuite.AnyFunSuite

class HashPointSuite extends AnyFunSuite {

    test("Test getName") {
        assertResult("AAAAAQAAAAIAAAAD")(new HashPoint(1, 2, 3).getName);
        assertResult("AAAAAQAAAAIAAAADAAAABAAAAAUAAAAGAAAABwAAAAgAAAAJAA" +
            "AACgAAAAsAAAAMAAAADQAAAA4AAAAPAAAAEAAAABEAAAASAAAA" +
            "EwAAABQAAAAVAAAAFgAAABcAAAAYAAAAGQAAABoAAAAbAAAAHA" +
            "AAAB0AAAAeAAAAHwAAACAAAAAhAAAAIgAAACMAAAAkAAAAJQAA" +
            "ACYAAAAnAAAAKAAAACkAAAAqAAAAKwAAACwAAAAtAAAALgAAAC" +
            "8AAAAwAAAAMQAAADIAAAAzAAAANAAAADUAAAA2AAAANwAAADgA" +
            "AAA5AAAAOgAAADsAAAA8AAAAPQAAAD4AAAA/AAAAQAAAAEEAAA" +
            "BCAAAAQwAAAEQAAABFAAAARgAAAEcAAABIAAAASQAAAEoAAABL" +
            "AAAATAAAAE0AAABOAAAATwAAAFAAAABRAAAAUgAAAFMAAABUAA" +
            "AAVQAAAFYAAABXAAAAWAAAAFkAAABaAAAAWwAAAFwAAABdAAAA" +
            "XgAAAF8AAABgAAAAYQAAAGIAAABjAAAAZA==")(new HashPoint((1 to 100)).getName)
    }
}
