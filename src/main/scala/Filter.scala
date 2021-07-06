package io.kohpai

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{Node, Triple}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Filter {

  val filterDir = "filter"
  val entitiesDir = "entities"
  val distDir = "dist"

  var triples = ""
  var nodes = ""
  var predicates = ""
  var nonPredicates = ""
  var triplesSrc = ""
  var entitiesSrc = ""
  var s_n_dist = ""
  var s_t_dist = ""

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(
        s"Filter-${args(0)}-${args(1)}"
      )
      .getOrCreate()

    triples = s"${args(0)}/$filterDir/triples"
    nodes = s"${args(0)}/$filterDir/nodes"
    predicates = s"${args(0)}/$filterDir/predicates"
    nonPredicates = s"${args(0)}/$filterDir/non-predicates"
    triplesSrc = s"${args(0)}/$entitiesDir/triples"
    entitiesSrc = s"${args(0)}/$entitiesDir/nodes"
    s_n_dist = s"${args(0)}/$distDir/s_n"
    s_t_dist = s"${args(0)}/$distDir/s_t"

    val mode = args(1)

    val sc = spark.sparkContext

    val triplesWithIndex: RDD[(Long, Triple)] = sc.objectFile(triplesSrc)
    val nodesWithIndex: RDD[(Long, Node)] = sc.objectFile(entitiesSrc)
    val dist: RDD[MatrixEntry] =
      if (mode == "TRIPLES") sc.objectFile(s_t_dist)
      else sc.objectFile(s_n_dist)

    val entries = dist.map(f => (f.i, f.value))
    val mean =
      entries.map(_._2).reduce { case (a, b) => a + b } / entries.count()

    if (mode == "TRIPLES") {
      filterEntries(mean, entries, triplesWithIndex)
    } else {
      filterEntries(args(1), mean, entries, nodesWithIndex, triplesWithIndex)
    }

    spark.stop()
  }

  def mapEntriesAboveMean[T](
      mean: Double,
      entries: RDD[(Long, Double)],
      entriesWithIndex: RDD[(Long, T)]
  ): RDD[(T, Double)] =
    entries
      .filter(_._2 > mean)
      .join(entriesWithIndex)
      .map(f => (f._2._2, f._2._1))

  def filterEntries(
      mean: Double,
      entries: RDD[(Long, Double)],
      triplesWithIndex: RDD[(Long, Triple)]
  ): Unit =
    mapEntriesAboveMean(mean, entries, triplesWithIndex)
      .map(_._1)
      .saveAsNTriplesFile(triples)

  def filterEntries(
      mode: String,
      mean: Double,
      entries: RDD[(Long, Double)],
      nodesWithIndex: RDD[(Long, Node)],
      triplesWithIndex: RDD[(Long, Triple)]
  ): Unit = {
    val entriesAboveMean = mapEntriesAboveMean(mean, entries, nodesWithIndex)

    val hashToNodePairs = entriesAboveMean.map(f => (f._1.hashCode(), f._1))
    val nodeToTriplePairs = triplesWithIndex
      .map(_._2)
      .flatMap(f =>
        mode match {
          case "NODES" =>
            Array(
              (f.getSubject.hashCode, f),
              (f.getPredicate.hashCode, f),
              (f.getObject.hashCode, f)
            )
          case "PREDICATES" =>
            Array(
              (f.getPredicate.hashCode, f)
            )
          case "NON_PREDICATES" =>
            Array(
              (f.getSubject.hashCode, f),
              (f.getObject.hashCode, f)
            )
          case _ =>
            throw new IllegalArgumentException(
              "wrong mode, please use TRIPLES, NODES, PREDICATES, or NON_PREDICATES"
            )
        }
      )

    hashToNodePairs
      .join(nodeToTriplePairs)
      .map(_._2._2)
      .distinct()
      .saveAsNTriplesFile(mode match {
        case "NODES"          => nodes
        case "PREDICATES"     => predicates
        case "NON_PREDICATES" => nonPredicates
        case _ =>
          throw new IllegalArgumentException(
            "wrong mode, please use TRIPLES, NODES, PREDICATES, or NON_PREDICATES"
          )
      })
  }
}
