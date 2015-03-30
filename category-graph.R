## category graph

# install.package("rgl")

library(igraph)
library(rgl)
library(data.table)
loadcmp("~/R/misc.Rc")

edges <- read.tsv.verbose("category-edges.tsv",header=FALSE,
                          col.names = c("src","dst","weight"))
vertices <- read.tsv.verbose("category-weights.tsv",header=FALSE,
                             col.names = c("category","weight"))
summary(edges$weight)
edges <- edges[order(edges$weight,decreasing=TRUE),]
head(edges)
tail(edges)

loglog.hist(edges$weight, breaks=40,panel.first=grid(),
            main="Edge Weight Histogram (ALL)",xlab="weight")

loglog.hist(edges$weight[edges$src != edges$dst], breaks=40,panel.first=grid(),
            main="Edge Weight Histogram (Distinct)",xlab="weight")

summary(vertices$weight)
loglog.hist(vertices$weight,panel.first=grid(),
            main="Category Weight Histogram",xlab="weight")
vertices <- vertices[order(vertices$weight,decreasing=TRUE),]
head(vertices)
tail(vertices)

## diag
edge.diag <- data.table(category = edges$src[edges$src == edges$dst],
                        weight.g = edges$weight[edges$src == edges$dst],
                        key="category")
vertices.dt <- as.data.table(vertices)
setkeyv(vertices.dt,"category")
vertices.dt <- edge.diag[vertices.dt]

(vertices.dt$top.level.cat <- sub("\\\\.*","",vertices.dt$category))
(vertices.dt$leaf.category <- sub("^.*\\\\([^\\]+)$","\\1",vertices.dt$category))

my.plot(log(vertices.dt$weight), log(vertices.dt$weight.g),
        panel.first=grid(),
        main="Category Weights: Total vs. Self-Link",
        xlab="log(total category weight)",
        ylab="log(self-link category weight)")

## drop loops
edges <- edges[edges$src != edges$dst,]

## scale links by vertex weights

edges$src.wtt <- vertices.dt[category == edges$src]$weight
edges$dst.wtt <- vertices.dt[category == edges$dst]$weight
edges$src.wtl <- vertices.dt[category == edges$src]$weight.g
edges$dst.wtl <- vertices.dt[category == edges$dst]$weight.g
edges$weight.t <- edges$weight/sqrt(edges$src.wtt * edges$dst.wtt)
edges$weight.l <- edges$weight/sqrt(edges$src.wtl * edges$dst.wtl)

summary(edges)
sum(edges$weight.t >= 1)        # 4324
sum(edges$weight.l >= 1)        # 36
quantile(edges$weight.t,c(0.5,0.7,0.8,0.9,0.95,0.99))
quantile(edges$weight.l,c(0.5,0.7,0.8,0.9,0.95,0.99))

sqrt(max(edges$weight.l) * min(edges$weight.l))

head(edges[order(edges$weight.t,decreasing=TRUE),])
head(edges[order(edges$weight.l,decreasing=TRUE),])

loglog.hist(edges$weight.t,breaks=40,xlab="weight",panel.first=grid(),
            main="Edge Weight Histogram (scaled by Total)")

capture.graphics(file="charts/edge-weight-hist.png",
  loglog.hist(edges$weight.l,breaks=40,xlab="weight",panel.first=grid(),
              main="Edge Weight Histogram (scaled by Self-Link)"))

## graph

(cg <- graph.data.frame(edges[order(edges$weight.t,decreasing=TRUE)[1:100],],
                        vertices = vertices.dt))

cg.conn.comp <- clusters(cg,mode="weak")
show.table(cg.conn.comp,"csize")

which(cg.conn.comp$csize > 10)  # 6
(big <- induced.subgraph(cg, which(cg.conn.comp$membership == 6)))

my.graph.plot <- function (graph, file=NULL) {
  tlc <- as.factor(V(graph)$top.level.cat)
  mycolors <- rainbow(length(levels(tlc)))
  capture.graphics(file=file,{
    plot(graph,
         main=paste(length(V(graph)),"vertices;",length(E(graph)),"edges"),
         edge.width = E(graph)$weight.l,
         edge.curved = TRUE,
         vertex.size = log(V(graph)$weight.g),
         vertex.label = V(graph)$leaf.category,
         vertex.label.color = "black",
         vertex.label.cex = 0.7,
         vertex.color = mycolors[as.integer(tlc)])
    legend(x="topleft", legend=levels(tlc), text.col=mycolors, cex=0.7)
  })
}

my.graph.plot(big,file="charts/component-41.png")

## whole graph
p.s.t(gr <- graph.data.frame(edges, vertices = vertices.dt))
clusters(gr,mode="weak")        # connected

(gru <- as.undirected(gr,edge.attr.comb=
                        list(weight="sum",weight.t="sum",weight.l="sum")))
(gr.tcommunities <- multilevel.community(gru, weight=E(gru)$weight.t))
sorted.table(gr.tcommunities$membership)
##   1   3   2   4
## 210  79  47  27
(gr.lcommunities <- multilevel.community(gru, weight=E(gru)$weight.l))
sorted.table(gr.lcommunities$membership)
##   2   1   5   3   4
## 115  81  71  49  47
(gr.wcommunities <- multilevel.community(gru, weight=E(gru)$weight))
sorted.table(gr.wcommunities$membership)
##   5   3   1   2   4
## 168  74  52  41  28

plot.communities <- function (gg, name) {
  header(name)
  print(gg)
  mlcl <- multilevel.community(gg, weight=E(gg)$weight.l)
  tab <- sorted.table(mlcl$membership)
  print(tab)
  ret <- lapply(names(tab), function (n) {
    sg <- induced.subgraph(gg, which(mlcl$membership == as.integer(n)))
    my.graph.plot(sg,file=paste0("charts/",name,".",n,".png"))
    list(graph=sg,
         subgraphs=(if (length(V(sg)) <= 20) NULL
                    else plot.communities(sg,paste0(name,".",n))))
  })
  names(ret) <- paste(name,names(tab),sep=".")
  ret
}
p.s.t(grs <- plot.communities(gru,"c"))

mycat <- "Style & Fashion\\Accessories\\Watches"

Filter(function(z) mycat %in% V(z$graph)$name, grs)
Filter(function(z) mycat %in% V(z$graph)$name, grs$c.3$subgraphs)
Filter(function(z) mycat %in% V(z$graph)$name, grs$c.3.2$subgraphs)


grs
mycat %in% V(gru)$name

(gr.l.4 <- induced.subgraph(gru, which(gr.lcommunities$membership == 4)))
p.s.t(my.graph.plot(gr.l.4))


## filter vertexes
(t1 <- graph.data.frame(edges[edges$weight.t >= 1,],
                        vertices = vertices.dt))

(t1.cc <- clusters(t1,mode="weak")) # 1 giant comp with 328 elements

## small components:
table(t1.cc$csize)
##  1   2 328
## 33   1   1

for (i in which(t1.cc$csize < 4))
  cat(t1.cc$csize[i],vertices.dt$category[t1.cc$membership == i],"\n")

## giant component:
(giant.comp <- which.max(t1.cc$csize))
(t1.giant <- induced.subgraph(t1, which(t1.cc$membership == giant.comp)))
(t1.communities <- multilevel.community(as.undirected(t1.giant)))
sorted.table(t1.communities$membership)

(t1.4 <- induced.subgraph(t1.giant, which(t1.communities$membership == 4)))
my.graph.plot(t1.4)
