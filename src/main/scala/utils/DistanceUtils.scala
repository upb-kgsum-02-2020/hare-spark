package io.kohpai
package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DistanceUtils {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Distance Utils")
      //      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val distance = euclideanDistance(sc.parallelize(Array(4.toDouble, -8.toDouble, -9.toDouble)),
      sc.parallelize(Array(2.toDouble, -3.toDouble, -5.toDouble)))

    println("Distancia: " + distance)

  }


  def euclideanDistance(left: RDD[Double], right: RDD[Double]): Double = {
    var l = left.zipWithIndex().map(f => (f._2, f._1))
    var r = right.zipWithIndex().map(f => (f._2, f._1))

    var d = l.join(r).map({ case (i, (l, r)) => (i, (l - r) * (l - r)) }).reduce({ case ((i, l), (x, r)) => (0, l + r) })

    Math.sqrt(d._2)
  }


}