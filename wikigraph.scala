// magnetic hackathon: import wikipedia into graphs

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

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
val page_MD = categories.rightOuterJoin(vertexes).map({ // 4,831,649
  case (pagen,(Some(cats),id)) => (id,(pagen,cats))
  case (pagen,(None,id)) => (id,(pagen,Array[(String,Double)]()))
}).cache()

val adjacency = edges.flatMap(s => { // 116,330,611
  val v = s.split("\t")
  val beg = v.head.toLong
  v.tail.groupBy(identity).map(t => (t._1, t._2.length)).toSeq.map({
    case (v,w) => Edge(beg,v.toLong,w)
  })
}).cache()

val wg = Graph.apply(page_MD,adjacency).cache()

// category ==> total weight (363 rows)
val category_table = categories.flatMap(t => t._2).reduceByKey(_+_).collect()

// project to categories (125784 rows)
val category_edges = adjacency.map({
  case Edge (src, dst, weight) => (src, (dst, weight))
}).join(page_MD).map({
  case (src, ((dst, weight), (srcn,srccats))) =>
    (dst, (weight, (src, srcn, srccats)))
}).join(page_MD).flatMap({
  case (dst, ((weight, (src, srcn, srccats)), (dstn, dstcats))) =>
    for { sc <- srccats; dc <- dstcats } yield
      ((sc._1, dc._1), weight * sc._2 * dc._2)
}).reduceByKey(_+_).collect()


// Prune output data
val pw = new java.io.PrintWriter("category-edges.tsv")
category_edges.foreach({ case ((c1,c2),w) =>
  pw.println(c1 + "\t" + c2 + "\t" + w.toString)})
pw.close

val pw = new java.io.PrintWriter("category-weights.tsv")
category_table.foreach({ case (c,w) => pw.println(c + "\t" + w.toString)})
pw.close


val qcSeedPages = sc.textFile("/user/dejan/QC_wikipedia.txt").filter(_.split("\t").length > 1).map( s => {
   val array = s.split("\t")
   (array.head,array.tail)
   }).cache()

