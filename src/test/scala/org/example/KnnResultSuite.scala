package org.example

import org.scalatest.funsuite.AnyFunSuite

class KnnResultSuite extends AnyFunSuite {

    test("Knn result 1") {
        var result = new KnnResult
        result = KnnResult.seqOp(3)(result, (5, 5))
        result = KnnResult.seqOp(3)(result, (2, 2))
        result = KnnResult.seqOp(3)(result, (1, 1))
        result = KnnResult.seqOp(3)(result, (3, 3))
        result = KnnResult.seqOp(3)(result, (4, 4))
        assertResult(Array((1, 1), (2, 2), (3, 3)))(result.sorted.toArray)

        result = new KnnResult
        result = KnnResult.seqOp(6)(result, (5, 5))
        result = KnnResult.seqOp(6)(result, (2, 2))
        result = KnnResult.seqOp(6)(result, (1, 1))
        result = KnnResult.seqOp(6)(result, (3, 3))
        result = KnnResult.seqOp(6)(result, (4, 4))
        assertResult(Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5)))(result.sorted.toArray)

        result = new KnnResult
        assertResult(Array())(result.sorted.toArray)
    }

    test("Knn result 4") {
        var result = new KnnResult
        result = KnnResult.seqOpOfList(6)(result, List((5, 1), (6, 2)))
        result = KnnResult.seqOpOfList(6)(result, List((2, 3), (8, 4)))
        result = KnnResult.seqOpOfList(6)(result, List((2, 5), (20, 6), (0, 7)))
        result = KnnResult.seqOpOfList(6)(result, List((7, 8), (2, 9), (70, 10)))
        result = KnnResult.seqOpOfList(6)(result, List())
        assertResult(Array((0, 7), (2, 3), (2, 5), (2, 9), (5, 1), (6, 2)))(result.sorted.toArray)
    }
}
