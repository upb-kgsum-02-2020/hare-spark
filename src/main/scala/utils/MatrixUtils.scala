package io.kohpai
package utils

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object MatrixUtils {


  /**
   *
   * Naive implementation to multiply sparse Matrix in Spark
   *
   */
  def coordinateMatrixMultiply(l: CoordinateMatrix, r: CoordinateMatrix): CoordinateMatrix = {

    val finalProduct = l.entries.map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
      .join(r.entries.map({ case MatrixEntry(j, k, w) => (j, (k, w)) }))
      .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
      .reduceByKey(_ + _)
      .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

    new CoordinateMatrix(finalProduct)
  }

  /**
   *
   * Naive implementatiom to sum sparse Matrix in Spark
   *
   */

  def coordinateMatrixSum(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix = {

    if ((leftMatrix.numCols() != rightMatrix.numCols()) || (leftMatrix.numCols() != rightMatrix.numCols())) {
      throw new IllegalArgumentException("Both Matrices need to have the same number of rows and columns")
    }

    val M_ = leftMatrix.entries.map({ case MatrixEntry(i, j, v) => ((i, j), v) })
    val N_ = rightMatrix.entries.map({ case MatrixEntry(j, k, w) => ((j, k), w) })

    val productEntries = M_
      .join(N_)
      .map({ case ((i, j), (v, w)) => ((i, j), (v + w)) })
      //            .reduceByKey(_ + _)
      .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })

    new CoordinateMatrix(productEntries)
  }

  def coordinateMatrixDivide(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix = {
    val inverse_rightMatrix = new CoordinateMatrix(rightMatrix.entries.map { x => new MatrixEntry(x.i, x.j, 1 / x.value) })

    coordinateMatrixMultiply(leftMatrix, inverse_rightMatrix)

  }


  def transpose(m: Array[Array[Double]]): Array[Array[Double]] = {
    (for {
      c <- m(0).indices
    } yield m.map(_ (c))).toArray
  }


  /**
   *
   * Multiply all numbers
   *
   */
  def multiplyMatrixByNumber(m: RowMatrix, n: Double) = {
    m.rows.map { x =>
      val s = x.toArray
      for (a <- 0 to s.size - 1) {
        s(a) = s(a) * n
      }
      Vectors.dense(s)
    }

  }

  /**
   *
   * Multiply all numbers
   *
   */
  def multiplyMatrixByNumber(m: CoordinateMatrix, n: Double): CoordinateMatrix = {
    new CoordinateMatrix(m.entries.map { x => new MatrixEntry(x.i, x.j, x.value * n) })

  }

  /**
   *
   * Divide all numbers
   *
   */
  def divideMatrixByNumber(m: CoordinateMatrix, n: Double): CoordinateMatrix = {
    new CoordinateMatrix(m.entries.map { x => new MatrixEntry(x.i, x.j, x.value / n) })

  }


  def transposeRowMatrix(m: RowMatrix): RowMatrix = {
    val transposedRowsRDD = m.rows.zipWithIndex.map { case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex) }
      .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
      .groupByKey
      .sortByKey().map(_._2) // sort rows and remove row indexes
      .map(buildRow) // restore order of elements in each row and remove column indexes
    new RowMatrix(transposedRowsRDD)
  }


  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map { case (value, colIndex) => (colIndex.toLong, (rowIndex, value)) }
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach { case (index, value) =>
      resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }

}