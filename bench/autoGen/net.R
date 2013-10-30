#make svgs for network data
options(error=traceback)
library(lattice)
interleave <- 
function(v1,v2) {
  ord1 <- 2*(1:length(v1))-1
  ord2 <- 2*(1:length(v2))
  c(v1,v2)[order(c(ord1,ord2))]
}

makeCuts <- 
function(cuts,pID) {
    i       <- 1
    tck_bot <- 2
    tck_top <- length(cuts) - 4 
    for (cut in cuts) {
        trellis.focus("panel",1,1, clip.off=TRUE)
        panel.abline(v=cut,lty="solid",col="black", lwd=1)
        side <- ifelse(i %% 2 == 0, "bottom", "top")
        rot  <- ifelse(i %% 2 == 0, c(45, "top"), 45) 
        tck <- 4
        if (! (i %in% c(length(cuts), length(cuts)-1))) {
            if (i %% 2 == 0) {
                tck <- tck_bot <- tck_bot + 1.75
            } else {
                tckr <- 1
                if (i > 3 && pID %in% c("kmeans","glm","glm2") && abs(cuts[i] - cuts[i - 2]) < 35) {
                    tckr <- 2.0
                }
                tck <- tck_top <- tck_top - tckr
            }
        } else {
            v <- abs(cuts[i] - cuts[i - 1]) > 60
            if (v) tck <- 4
        }
        panel.axis(side=side, at = cut, label=labels[i], rot = rot, outside=TRUE, tck = tck, which.half="upper", ticks=T, text.cex=.6)
        trellis.unfocus()
        i <- i + 1 
    } 
}

args        <- commandArgs(TRUE)
pID         <- args[4]
mID         <- paste("192.168.1.",args[3], sep = "")
gID         <- paste(paste("net_", paste(pID, args[3], sep="_"),sep=""), ".svg", sep="")
p1          <- paste(pID, 'bench.csv', sep = "") 
p2          <- paste('/benchmarks/', p1, sep = "") 
res         <- paste(args[5], p2, sep = "") 
height      <- 5
if (pID == "gbm")     height = 2.75
if (pID == "kmeans")  height = 4 
if (pID == "summary") height = 4
if (pID == "pca")     height = 4

svg(paste('./svgs/',gID,sep=""), width = 12, height = height)

netR          <- read.csv(args[1])
netR$time.s.  <- netR$time.s. - min(netR$time.s.)
netT          <- read.csv(args[2])
netT$time.s.  <- netT$time.s. - min(netT$time.s.)
trellis.par.set(layout.widths = list(left.padding = 10))
trellis.par.set(par.main.text = list(alpha = 0.2))
timeR <- netR$time.s.
idR <- which(diff(timeR) >= 9)
timeT <- netT$time.s.
idT <- which(diff(timeT) >= 9)

netR <- netR[idR,]
netT <- netT[idT,]

f <- function(col) {
    return(diff(col)/10)
}
dfR  <- rbind(c(0,0,0,0), apply(netR[,-c(1,2)],2,f))
dfT  <- rbind(c(0,0,0,0), apply(netT[,-c(1,2)],2,f))
netR <- as.data.frame(cbind(netR$time.s., dfR))
netT <- as.data.frame(cbind(netT$time.s., dfT))
colnames(netR) <- colnames(netT) <- c("time.s.", "bytes", "packets", "errs", "drop")
grx <-      c(rep('rx-packets', nrow(netR)))

gtx <-      c(rep('tx-packets', nrow(netT)))
rx <- netR$packets/1000
tx <- netT$packets/1000
drx <- data.frame(time.s. = netR$time.s., net = rx, groups = grx)
dtx <- data.frame(time.s. = netT$time.s., net = tx, groups = gtx)

network <- data.frame(rbind(drx,dtx))
aspect     <- 1/12
my.plot    <- xyplot(net ~ time.s.,
                         aspect   = aspect,
                         groups   = groups,
                         data     = network,
                         type     = "a",
                         key      = list(text=list(levels(network$groups)), space='top', points=FALSE,
                                         lines=list(col=c('red', 'dark green'))),
                         col      = c('red', 'dark green'),
                         main     = paste(toupper(pID), paste("Network Packets ", mID, sep=""),sep=" "),
                         xlab     = "time(s)",
                         ylab     = "Packets (thousands/sec)",
                         ylim     = c(0,max(network$net)),
                         xlim     = c(min(netR$time.s.),max(netR$time.s.)))


print(my.plot, clip.off=TRUE)
cuts       <- 0

if (pID == "summary") {
labels     <- c("EndParseAir1x",    paste("End",paste(toupper(args[4]),"Air1x",   sep=""), sep=""), "EndParseAir10x",  paste("End",paste(toupper(args[4]), "Air10x",  sep=""), sep=""),
                "EndparseAllB1x",   paste("End",paste(toupper(args[4]),"AllB1x",  sep=""), sep=""), "EndparseAllB10x", paste("End",paste(toupper(args[4]), "AllB10x", sep=""), sep=""),
                "EndparseAllB100x", paste("End",paste(toupper(args[4]),"AllB100x",sep=""), sep=""), "EndparseAir100x", paste("End",paste(toupper(args[4]), "Air100x", sep=""), sep=""))

   results <- read.csv(res)
   x1      <- results$parseWallTime 
   x2      <- results[paste(args[4],'BuildTime',sep="")][,1]
   x3      <- interleave(x1,x2)
   cuts    <- cumsum(x3)
   cuts    <- cuts + min(netR$time.s.)
   makeCuts(cuts,pID)

}

if (pID == "gbm") {
labels     <- c("EndParseAir1x",    paste("End",paste(toupper(args[4]),"Air1x",   sep=""), sep=""), "EndParseAir10x",  paste("End",paste(toupper(args[4]), "Air10x",  sep=""), sep=""),
                "EndparseAllB1x",   paste("End",paste(toupper(args[4]),"AllB1x",  sep=""), sep=""), "EndparseAllB10x", paste("End",paste(toupper(args[4]), "AllB10x", sep=""), sep=""),
                "EndparseAllB100x", paste("End",paste(toupper(args[4]),"AllB100x",sep=""), sep=""), "EndparseAir100x", paste("End",paste(toupper(args[4]), "Air100x", sep=""), sep=""))

   results <- read.csv(res)
   x1      <- results$trainParseWallTime 
   x2      <- results[paste(args[4],'BuildTime',sep="")][,1]
   x3      <- interleave(x1,x2)
   cuts    <- cumsum(x3)
   cuts    <- cuts + min(netR$time.s.)
   makeCuts(cuts,pID)
}

if (pID %in% c("kmeans", "pca")) {
labels     <- c("EndParseAir1x",    paste("End",paste(toupper(args[4]),"Air1x",   sep=""), sep=""), "EndParseAir10x",  paste("End",paste(toupper(args[4]), "Air10x",  sep=""), sep=""),
                "EndparseAllB1x",   paste("End",paste(toupper(args[4]),"AllB1x",  sep=""), sep=""), "EndparseAllB10x", paste("End",paste(toupper(args[4]), "AllB10x", sep=""), sep=""),
                "EndparseAllB100x", paste("End",paste(toupper(args[4]),"AllB100x",sep=""), sep=""), "EndparseAir100x", paste("End",paste(toupper(args[4]), "Air100x", sep=""), sep=""))

   results <- read.csv(res)
   x1      <- results$parseWallTime
   x2      <- results[paste(args[4],'BuildTime',sep="")][,1]
   x3      <- interleave(x1,x2)
   cuts    <- cumsum(x3)
   cuts    <- cuts + min(netR$time.s.)
   makeCuts(cuts,pID)
}

if (pID %in% c("glm", "glm2")) {
labels     <- c("EndParseTestAir", 
                "EndParseAir1x",   paste("End",paste(toupper(args[4]),"Air1x",   sep=""), sep=""),
                "EndParseTestAir",
                "EndParseAir10x",  paste("End",paste(toupper(args[4]), "Air10x", sep=""), sep=""),
                "EndParseTestAllB",
                "EndparseAllB1x",  paste("End",paste(toupper(args[4]),"AllB1x",  sep=""), sep=""),
                "EndParseTestAllB",
                "EndparseAllB10x", paste("End",paste(toupper(args[4]), "AllB10x", sep=""), sep=""),
                "EndParseTestAllB",
                "EndparseAllB100x", paste("End",paste(toupper(args[4]),"AllB100x",sep=""), sep=""),
                "EndParseTestAir",
                "EndparseAir100x", paste("End",paste(toupper(args[4]), "Air100x", sep=""), sep=""))

    results <- read.csv(res)
    x1      <- results$testParseWallTime
    x2      <- results$trainParseWallTime
    x3      <- results[paste(args[4],'BuildTime',sep="")][,1]
    x4      <- c(rbind(x1,x2,x3))
    cuts    <- cumsum(x4)
    cuts    <- cuts + min(netR$time.s.)
    makeCuts(cuts,pID)
}

dev.off()
