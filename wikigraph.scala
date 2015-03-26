// magnetic hackathon: import wikipedia into graphs

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

def kwcPrFileName(fileType:String, category:String): String = {
  "kwc-"+fileType+"-"+ category.replace('\\','-').replace('/','-')+".tsv"
}

object PageRankOrder extends Ordering[(Long, (((String, scala.collection.immutable.Map[String,Double]), Double), Int))] {
  override def compare(x: (Long, (((String, Map[String, Double]), Double), Int)), y: (Long, (((String, Map[String, Double]), Double), Int))): Int = y._2._1._2.compareTo(x._2._1._2)
}

object IndegreeOrder extends Ordering[(Long, (((String, scala.collection.immutable.Map[String,Double]), Double), Int))] {
  override def compare(x: (Long, (((String, Map[String, Double]), Double), Int)), y: (Long, (((String, Map[String, Double]), Double), Int))): Int = y._2._2.compareTo(x._2._2)
}

val junkCategoriesTreshold = Map("World Localities" -> 0.95)

def removeJunkCategories(junkCategoriesTreshold: Map[String, Double], categories: Array[(String, Double)]): Array[(String, Double)] = {
  categories.toList.filter { case (name: String, score: Double) => junkCategoriesTreshold.get(name).getOrElse(Double.MinValue) < score}.toArray
}

val home = "/user/misos/wikigraph/"

val edges = sc.textFile(home+"edges.txt",300).cache()
// edges.partitions.length ==> 10
// edges.count() ==> 4809635

val categories = sc.textFile(home + "categories").map(s => {
  // 4657693
  val v = s.split("\t")
  (v.head, v.tail.map(cw => {
    val cwv = cw.split(":")
    (cwv(0), cwv(1).toDouble) // (category name, category weight)
  })) // (page name, Array((category name, category weight)))
}).map { case (pageName: String, categoryNames: Array[(String, Double)]) =>
  (pageName, removeJunkCategories(junkCategoriesTreshold, categoryNames)) }

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

for((my_category,weight) <- category_table) {
  val my_page_MD = page_MD.filter({
    case (id, (pagen, cats)) => cats.get(my_category).getOrElse(Double.MinValue) == cats.maxBy(_._2)._2
  })

  val my_adjacency = adjacency.map({ case Edge(src, dst, w) =>
    (src, (dst, w))
  }).join(my_page_MD).map({ case (src, ((dst, w), md)) =>
    (dst, (src, w))
  }).join(my_page_MD).map({ case (dst, ((src, w), md)) =>
    Edge(src, dst, w)
  }).cache()

  val my_graph = Graph.apply(my_page_MD, my_adjacency).cache()
  val my_pr = my_graph.staticPageRank(5).cache().vertices
  val my_indeg = my_graph.inDegrees
  val my_pages = my_page_MD.join(my_pr).join(my_indeg).cache()
  val page_rank_top = my_pages.takeOrdered(100)(PageRankOrder).toList
  val indegree_rank_top = my_pages.takeOrdered(100)(IndegreeOrder).toList


  val pwpr = new java.io.PrintWriter(kwcPrFileName("pr",my_category))
  page_rank_top.foreach({ case (id, (((pagen, cats), pr), indeg)) =>
    pwpr.println(pagen + "\t" + pr.toString + "\t" + indeg.toString + "\t" +
      cats.map({ case (c, w) => c + ":" + w.toString }).mkString("", ",", ""))
  })
  pwpr.close

  val pw = new java.io.PrintWriter(kwcPrFileName("indegrees",my_category))
  indegree_rank_top.foreach({ case (id, (((pagen, cats), pr), indeg)) =>
    pw.println(pagen + "\t" + pr.toString + "\t" + indeg.toString + "\t" +
      cats.map({ case (c, w) => c + ":" + w.toString }).mkString("", ",", ""))
  })
  pw.close

}



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


/*val qcSeedPages = sc.textFile("/user/dejan/QC_wikipedia.txt").filter(_.split("\t").length > 1).map( s => {
   val array = s.split("\t")
   (array.head,array.tail)
   }).cache()
*/

val rawCategories = sc.textFile("/user/dejan/QC_wikipedia.txt").filter(_.split("\t").length > 1).map( s => s.split("\t").head).collect

