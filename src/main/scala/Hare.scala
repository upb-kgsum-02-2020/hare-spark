package io.kohpai

import utils.{DistanceUtils, MatrixUtils}

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession

import java.math.BigDecimal

object Hare {

  val df = 0.85

  val matricesDir = "matrices"
  val distDir = "dist"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(
        "HARE-" + args(0).substring(args(0).lastIndexOf("/") + 1)
      )
      .getOrCreate()

    val w_path = s"${args(0)}/$matricesDir/w"
    val f_path = s"${args(0)}/$matricesDir/f"
    val s_t_dist = s"${args(0)}/$distDir/s_t"
    val s_n_dist = s"${args(0)}/$distDir/s_n"

    val sc = spark.sparkContext

    // W and F transition matrices for finding P(N) -> S(N) -> S(T) -> S
    val w = new CoordinateMatrix(sc.objectFile(w_path))
    val f = new CoordinateMatrix(sc.objectFile(f_path))

    // transition matrix P(N)
    val p_n = MatrixUtils.coordinateMatrixMultiply(f, w)

    val s_n_v = f.numRows()

    // S_0 initial entry value
    val s_i = f.numCols().toDouble / (w
      .numCols()
      .toDouble * (f.numCols().toDouble + w.numCols().toDouble))

    // sequence of entities
    val t = sc.parallelize(0 until f.numRows().toInt)

    // initialize S_0 (column vector)
    var s_n_final = new CoordinateMatrix(t.map { x =>
      MatrixEntry(x, 0, s_i)
    })

    // I
    val matrix_i = new CoordinateMatrix(t.map { x =>
      MatrixEntry(x, 0, 1)
    })

    var s_t_final = s_n_final
    var s_n_previous = s_n_final

    val epsilon = new BigDecimal(0.001)
    var distance = new BigDecimal(1)

    val a = MatrixUtils.multiplyMatrixByNumber(p_n, df).transpose()
    val b = MatrixUtils.divideMatrixByNumber(
      MatrixUtils.multiplyMatrixByNumber(matrix_i, 1 - df),
      s_n_v.toDouble
    )

    var iter = 0
    while (distance.compareTo(epsilon) == 1 && iter < 1000) {
      s_n_previous = s_n_final

      s_n_final = MatrixUtils.coordinateMatrixSum(
        MatrixUtils.coordinateMatrixMultiply(a, s_n_previous),
        b
      )

      distance = new BigDecimal(
        DistanceUtils
          .euclideanDistance(
            s_n_final.entries.map(f => f.value),
            s_n_previous.entries.map(f => f.value)
          )
      )

      iter = iter + 1
    }

    System.gc()
    s_t_final = MatrixUtils.coordinateMatrixMultiply(f.transpose(), s_n_final)

    s_n_final.entries.saveAsObjectFile(s_n_dist)
    s_t_final.entries.saveAsObjectFile(s_t_dist)

    spark.stop()
  }
}
