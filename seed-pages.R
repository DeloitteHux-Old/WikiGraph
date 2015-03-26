## seed pages

mycolnames <- c("title","pr","indegrees","categories")
ferrari <- read.tsv.verbose("ferrari.tsv",header=FALSE,
                            col.names=mycolnames)
loglog.hist(ferrari$pr)
loglog.hist(ferrari$indegrees)
head(ferrari[order(ferrari$pr,decreasing=TRUE),])
head(ferrari[order(ferrari$indegrees,decreasing=TRUE),])

top.pages <- function (catn) {
  catn.p <- sub("[\\\\/]","-",catn) # printable
  lapply(c("pr","indegrees"), function (kind) {
    frame <- read.tsv.verbose(paste0("dejan/kwc-",kind,"-",catn.p,".tsv"),
                              header=FALSE,col.names=mycolnames)
    ldf <- lapply(strsplit(frame$categories,","), function(l) {
      mx <- do.call(rbind,strsplit(l,":"))
      data.frame(cat = mx[,1], wt = as.double(mx[,2]))
    })
    frame$weight <- sapply(ldf, function(df) df$wt[df$cat == catn])
    frame$topcat <- sapply(ldf, function(df) df$cat[which.max(df$wt)])
    frame$categories <- NULL
    print(head(frame[order(frame[[kind]]*frame$weight,decreasing=TRUE),]))
    frame
  })
}
top.pages("Automotive\\Convertible & Sports cars")
top.pages("Education\\Continuing Education")
top.pages("Business\\Agriculture & Forestry")
top.pages("Sports\\Water Sports")
top.pages("Automotive\\Convertible & Sports cars")
