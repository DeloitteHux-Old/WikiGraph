// magnetic hackathon: import wikipedia into graphs

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

val home = "/user/misos/wikigraph/"

val edges = sc.textFile(home+"edges.txt").cache()
// edges.partitions.length ==> 10
// edges.count() ==> 4809635

val categories = sc.textFile(home+"categories").map(s => { // 4657693
  val v = s.split("\t")
  (v.head,v.tail.map(cw => {
    val cwv = cw.split(":")
    (cwv(0), cwv(1).toDouble) // (category name, category weight)
  })) // (page name, Array((category name, category weight)))
})

val vertexes = sc.textFile(home+"vertex_ids.txt").map(s => { // 4831649
  val v = s.split("\t")
  (v(0),v(1).toLong)}).cache() // (page name, page ID)

// use fullOuterJoin to also get vertixes without ID
val page_MD = categories.rightOuterJoin(vertexes).map(_ match {
  case (pagen,(Some(cats),id)) => (id,(pagen,cats))
  case (pagen,(None,id)) => (id,(pagen,Array()))
}).cache()

val adjacency = edges.flatMap(s => { // 116330611
  val v = s.split("\t")
  val beg = v.head.toLong
  v.tail.groupBy(identity).map(t => (t._1, t._2.length)).toSeq.map(_ match {
    case (v,w) => Edge(beg,v.toLong,w)
  })
}).cache()

import org.apache.spark.graphx._

val wg = Graph.apply(page_MD,adjacency).cache()

// category ==> total weight
val category_table = categories.flatMap(t => t._2).reduceByKey(_+_).collect()
