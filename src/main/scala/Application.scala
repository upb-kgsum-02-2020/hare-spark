package io.kohpai

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

object Application {

  def main(args: Array[String]) {
    val input = args(0)
    val output = args(1)

    val spark = SparkSession.builder
      .appName(s"Triple reader example with: $input")
      .getOrCreate()

    println("======================================")
    println("|        Triple reader example       |")
    println("======================================")

    val lang = Lang.NTRIPLES
    val triples = spark.rdf(lang)(input)

    triples.take(5).foreach(println(_))

    triples.saveAsNTriplesFile(output)

    spark.stop

  }
}
