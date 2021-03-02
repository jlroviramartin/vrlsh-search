package org.example

import org.apache.spark.ml.linalg.Vectors
import org.example.evaluators.{DefaultHasher, EuclideanHashEvaluator, Hash, HashPoint, LineEvaluator}
import org.scalatest.funsuite.AnyFunSuite

import java.util.NoSuchElementException

class DefaultHasherSuite extends AnyFunSuite {

    test("Default hasher evaluator") {
        assertResult(
            Seq(new HashPoint(6, 8, 0), new HashPoint(6, 8, 1))
        )(
            new DefaultHasher(
                new EuclideanHashEvaluator(Array(
                    new LineEvaluator(1.0, 2.0, 3.0),
                    new LineEvaluator(2.0, 3.0, 3.0))),
                new EuclideanHashEvaluator(Array(
                    new LineEvaluator(1.0, 2.0, 3.0),
                    new LineEvaluator(2.0, 3.0, 3.0)))
            ).hash(Vectors.dense(1.0, 1.0), 1)
        );

        assertResult(
            Seq(new HashPoint(18, 26, 0), new HashPoint(18, 26, 1))
        )(
            new DefaultHasher(
                new EuclideanHashEvaluator(Array(
                    new LineEvaluator(1.0, 2.0, 3.0),
                    new LineEvaluator(2.0, 3.0, 3.0))),
                new EuclideanHashEvaluator(Array(
                    new LineEvaluator(1.0, 2.0, 3.0),
                    new LineEvaluator(2.0, 3.0, 3.0)))
            ).hash(Vectors.dense(2.0, 2.0), 1.0 / 2.0)
        );

        assertResult(
            Seq(new HashPoint(18, 26, 0), new HashPoint(42, 66, 1))
        )(
            new DefaultHasher(
                new EuclideanHashEvaluator(Array(
                    new LineEvaluator(1.0, 2.0, 3.0),
                    new LineEvaluator(2.0, 3.0, 3.0))),
                new EuclideanHashEvaluator(Array(
                    new LineEvaluator(4.0, 5.0, 3.0),
                    new LineEvaluator(7.0, 8.0, 3.0)))
            ).hash(Vectors.dense(2.0, 2.0), 1.0 / 2.0)
        );
    }

    test("Default hasher in Map") {
        val map = collection.mutable.Map[Hash, String]();
        map.put(new HashPoint(1, 2, 3), "Hola");
        map.put(new HashPoint(4, 5, 6), "mundo");
        assertResult("Hola")(map(new HashPoint(1, 2, 3)));
        assertResult("mundo")(map(new HashPoint(4, 5, 6)));
        assertThrows[NoSuchElementException](map(new HashPoint(7, 8, 9)));
    }

    test("Default hasher in Map (2)") {
        val map = collection.mutable.Map[Seq[Hash], String]();
        map.put(Seq(new HashPoint(1, 2, 3), new HashPoint(4, 5, 6)), "Hola");
        map.put(Seq(new HashPoint(7, 8, 9)), "mundo");
        assertResult("Hola")(map(Seq(new HashPoint(1, 2, 3), new HashPoint(4, 5, 6))));
        assertResult("mundo")(map(Seq(new HashPoint(7, 8, 9))));
        assertThrows[NoSuchElementException](map(Seq(new HashPoint(10, 11, 12))));
    }

    test("Default equals/hash") {

        assert((1 until 5).toArray.toSeq.equals((1 until 5).toArray.toSeq));
        assert((1 until 5).toArray.toSeq.hashCode == (1 until 5).toArray.toSeq.hashCode);

        val o1 = new DefaultHasher(
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0))),
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(4.0, 5.0, 3.0),
                new LineEvaluator(7.0, 8.0, 3.0))))

        val o2 = new DefaultHasher(
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0))),
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(4.0, 5.0, 3.0),
                new LineEvaluator(7.0, 8.0, 3.0))))

        assert(o1.equals(o2));
        assert(o1.hashCode == o2.hashCode);

        val o3 = new DefaultHasher(
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 2.0, 3.0),
                new LineEvaluator(2.0, 3.0, 3.0))),
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(4.0, 5.0, 3.0),
                new LineEvaluator(7.0, 8.0, 3.0))),
            new EuclideanHashEvaluator(Array(
                new LineEvaluator(1.0, 1.0, 3.0),
                new LineEvaluator(2.0, 2.0, 3.0))))

        assert(!o1.equals(o3));
        assert(o1.hashCode != o3.hashCode);
    }
}
