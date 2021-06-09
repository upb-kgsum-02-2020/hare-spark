package io.kohpai

import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object MatricesGenerator {


  var sourcePath = ""
  var w_dest = "/matrices/w"
  var f_dest = "/matrices/f"
  var edges_triples_dest = "/matrices/edges_triples"
  var edges_resources_dest = "/matrices/edges_resources"
  var statistics_dest = "/statistics"
  var entities_dest = "/entities"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MatrixGenerator")
      .getOrCreate()

    sourcePath = args(0) + sourcePath
    w_dest = args(1) + w_dest
    f_dest = args(1) + f_dest
    edges_triples_dest = args(1) + edges_triples_dest
    edges_resources_dest = args(1) + edges_resources_dest
    statistics_dest = args(1) + statistics_dest
    entities_dest = args(1) + entities_dest

    val t1 = System.currentTimeMillis()

    val triples = spark.rdf(Lang.NTRIPLES)(sourcePath)

    val subjects = triples.map(_.getSubject)
    val predicates = triples.map(_.getPredicate)
    val objects = triples.map(_.getObject)

    val triplesWithIndex = triples.distinct().zipWithIndex()
    val entitiesWithIndex = subjects.union(predicates).union(objects).distinct().zipWithIndex()

    println("Zipped triples and entities")

    val switchPlacesTuple = (x: (Any, Any)) => (x._2, x._1)
    triplesWithIndex.map(switchPlacesTuple).saveAsObjectFile(s"$entities_dest/triples")
    entitiesWithIndex.map(switchPlacesTuple).saveAsObjectFile(s"$entities_dest/entities")

    println("Saved nodes and triples")

    val total_edges = triples.flatMap { f =>
      {
        val fStr = f.toString
        Array(
          (fStr, f.getSubject.toString),
          (fStr, f.getPredicate.toString),
          (fStr, f.getObject.toString)
        )
      }
    }
    val final_matrix = total_edges
      .join(
        triplesWithIndex.map[(String, Long)](f => (f._1.toString, f._2))
      ) // (triple, (sub_pred_obj, index_t))
      .map(_._2) // (sub, index_t) or (pred, index_t) or (obj, index_t)
      .join(
        entitiesWithIndex.map[(String, Long)](f => (f._1.toString, f._2))
      ) // (sub_pred_obj, (index_t, index_e))
      .map(_._2) // (index_t, index_e)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val map_edges_triples = final_matrix.groupBy(x => x._1)
    val map_edges_resources = final_matrix.groupBy(x => x._2)

    val count_triples = map_edges_triples.count
    val count_resources = map_edges_resources.count

    val parseTime = (System.currentTimeMillis() - t1) / 1000

    val t2 = System.currentTimeMillis()

    val w = map_edges_triples
      .flatMap(x => x._2.map(v => MatrixEntry(v._1, v._2, 1.0 / x._2.size.toDouble)))
      .filter(f => f != null)

    val f = map_edges_resources
      .flatMap(x => x._2.map(v => MatrixEntry(v._2, v._1, 1.0 / x._2.size.toDouble)))
      .filter(f => f != null)

    println("Constructed matrices")

    val toReadable = (x: MatrixEntry) => s"${x.i},${x.j},${x.value}"

    w.saveAsObjectFile(w_dest)
    f.saveAsObjectFile(f_dest)

    w.map(toReadable).saveAsTextFile(s"${w_dest}_readable")
    f.map(toReadable).saveAsTextFile(s"${f_dest}_readable")

    println("Saved matrices")


    val matrixTime = (System.currentTimeMillis() - t2) / 1000

    val statistics = new ListBuffer[String]()
    statistics += "Number of Triples: " + count_triples
    statistics += "Number of Entities: " + count_resources
    statistics += "Parsing File Time: " + parseTime
    statistics += "Matrix Generation Time: " + matrixTime

    val rdd_statistics = spark.sparkContext.parallelize(statistics)
    rdd_statistics.repartition(1).saveAsTextFile(statistics_dest)
    spark.stop()

  }

}