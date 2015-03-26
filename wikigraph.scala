// magnetic hackathon: import wikipedia into graphs

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

val home = "/user/misos/wikigraph/"

val edges = sc.textFile(home+"edges.txt",300).cache()
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

// use fullOuterJoin to also get vertexes without ID
val page_MD = categories.rightOuterJoin(vertexes).map({ // 4,831,649
  case (pagen,(Some(cats),id)) => (id,(pagen,
    cats.groupBy(_._1).map({ case (cat,wt) => (cat,wt(0)._2)})))
  case (pagen,(None,id)) => (id,(pagen,Map[String,Double]()))
}).cache()

val adjacency = edges.flatMap(s => { // 116,330,611
  val v = s.split("\t")
  val beg = v.head.toLong
  v.tail.groupBy(identity).map(t => (t._1, t._2.length)).toSeq.map({
    case (v,w) => Edge(beg,v.toLong,w)
  })
}).cache()

// whole graph - don't need it!
// val wg = Graph.apply(page_MD,adjacency).cache()

// category ==> total weight (363 rows)
val category_table = categories.flatMap(t => t._2).reduceByKey(_+_).collect()

// subgraph
val my_category = "Automotive\\Manufacturers\\Ferrari"
val my_page_MD = page_MD.filter({ // 2867
  case (id,(pagen,cats)) => cats.contains(my_category)
})
val my_adjacency = adjacency.map({case Edge(src,dst,w) =>
  (src,(dst,w))}).join(my_page_MD).map({case (src,((dst,w),md)) =>
    (dst,(src,w))}).join(my_page_MD).map({case (dst,((src,w),md)) =>
      Edge(src,dst,w)}).cache() // 59733

val my_graph = Graph.apply(my_page_MD,my_adjacency).cache()
val my_pr = my_graph.staticPageRank(5).cache().vertices
val my_indeg = my_graph.inDegrees
val my_pages = my_page_MD.join(my_pr).join(my_indeg)

val pw = new java.io.PrintWriter("ferrari.tsv")
my_pages.foreach({ case (id,(((pagen,cats),pr),indeg)) =>
  println(pagen + "\t" + pr.toString + "\t" + indeg.toString + "\t" +
    cats.map({case (c,w) => c+":"+w.toString}).mkString("",",",""))})
pw.close


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

