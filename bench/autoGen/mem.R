#make svgs for memory data
#options(error=traceback)
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
pID         <- args[3]
mID         <- paste("192.168.1.",args[2], sep = "")
gID         <- paste(paste("mem_", paste(pID, args[2], sep="_"),sep=""), ".svg", sep="")
p1          <- paste(args[3], 'bench.csv', sep = "") 
p2          <- paste('/benchmarks/', p1, sep = "") 
res         <- paste(args[4], p2, sep = "") 
height      <- 5
if (pID == "gbm")     height = 2.75
if (pID == "kmeans")  height = 4 
if (pID == "summary") height = 4
if (pID == "pca")     height = 4

svg(paste('./svgs/',gID,sep=""), width = 12, height = height)

MEM         <- read.csv(args[1])
MEM$time.s. <- MEM$time.s. - min(MEM$time.s.)
MEM$MemFree <- max(MEM$MemFree) - MEM$MemFree
aspect      <- 1/12
trellis.par.set(layout.widths = list(left.padding = 10))
trellis.par.set(par.main.text = list(alpha = 0.2))
my.plot     <- xyplot(RSS/(1024*1024) ~ time.s.,
                  aspect   = aspect,
                  data     = MEM,
                  type     = "a",
                  col      = c("blue"),#, "blue"),
                  main     = paste(toupper(pID), paste("H2O's RSS Memory on ", mID, sep=""),sep=" "),
                  xlab     = "time(s)",
                  ylab     = "Memory (GB)",
                  ylim     = c(-1,MEM[1,2]/(1024*1024)),
                  xlim     = c(min(MEM$time.s.),max(MEM$time.s.)))
print(my.plot, clip.off=TRUE)

cuts        <- 0

if (pID == "summary") {
labels     <- c("EndParseAir1x",    paste("End",paste(toupper(args[3]),"Air1x",   sep=""), sep=""), "EndParseAir10x",  paste("End",paste(toupper(args[3]), "Air10x",  sep=""), sep=""),
                "EndparseAllB1x",   paste("End",paste(toupper(args[3]),"AllB1x",  sep=""), sep=""), "EndparseAllB10x", paste("End",paste(toupper(args[3]), "AllB10x", sep=""), sep=""),
                "EndparseAllB100x", paste("End",paste(toupper(args[3]),"AllB100x",sep=""), sep=""), "EndparseAir100x", paste("End",paste(toupper(args[3]), "Air100x", sep=""), sep=""))

   results <- read.csv(res)
   x1      <- results$parseWallTime
   x2      <- results[paste(args[3],'BuildTime',sep="")][,1]
   x3      <- interleave(x1,x2)
   cuts    <- cumsum(x3)
   cuts    <- cuts + min(MEM$time.s.)
   makeCuts(cuts,pID)

}

if (pID == "gbm") {
labels     <- c("EndParseAir1x",    paste("End",paste(toupper(args[3]),"Air1x",   sep=""), sep=""), "EndParseAir10x",  paste("End",paste(toupper(args[3]), "Air10x",  sep=""), sep=""),
                "EndparseAllB1x",   paste("End",paste(toupper(args[3]),"AllB1x",  sep=""), sep=""), "EndparseAllB10x", paste("End",paste(toupper(args[3]), "AllB10x", sep=""), sep=""),
                "EndparseAllB100x", paste("End",paste(toupper(args[3]),"AllB100x",sep=""), sep=""), "EndparseAir100x", paste("End",paste(toupper(args[3]), "Air100x", sep=""), sep=""))

   results <- read.csv(res)
   x1      <- results$trainParseWallTime 
   x2      <- results[paste(args[3],'BuildTime',sep="")][,1]
   x3      <- interleave(x1,x2)
   cuts    <- cumsum(x3)
   cuts    <- cuts + min(MEM$time.s.)
   makeCuts(cuts,pID)
}

if (pID %in% c("kmeans", "pca")) {
labels     <- c("EndParseAir1x",    paste("End",paste(toupper(args[3]),"Air1x",   sep=""), sep=""), "EndParseAir10x",  paste("End",paste(toupper(args[3]), "Air10x",  sep=""), sep=""),
                "EndparseAllB1x",   paste("End",paste(toupper(args[3]),"AllB1x",  sep=""), sep=""), "EndparseAllB10x", paste("End",paste(toupper(args[3]), "AllB10x", sep=""), sep=""),
                "EndparseAllB100x", paste("End",paste(toupper(args[3]),"AllB100x",sep=""), sep=""), "EndparseAir100x", paste("End",paste(toupper(args[3]), "Air100x", sep=""), sep=""))

   results <- read.csv(res)
   x1      <- results$parseWallTime
   x2      <- results[paste(args[3],'BuildTime',sep="")][,1]
   x3      <- interleave(x1,x2)
   cuts    <- cumsum(x3)
   cuts    <- cuts + min(MEM$time.s.)
   makeCuts(cuts,pID)
}

if (pID %in% c("glm", "glm2")) {
labels     <- c("EndParseTestAir", 
                "EndParseAir1x",   paste("End",paste(toupper(args[3]),"Air1x",   sep=""), sep=""),
                "EndParseTestAir",
                "EndParseAir10x",  paste("End",paste(toupper(args[3]), "Air10x", sep=""), sep=""),
                "EndParseTestAllB",
                "EndparseAllB1x",  paste("End",paste(toupper(args[3]),"AllB1x",  sep=""), sep=""),
                "EndParseTestAllB",
                "EndparseAllB10x", paste("End",paste(toupper(args[3]), "AllB10x", sep=""), sep=""),
                "EndParseTestAllB",
                "EndparseAllB100x", paste("End",paste(toupper(args[3]),"AllB100x",sep=""), sep=""),
                "EndParseTestAir",
                "EndparseAir100x", paste("End",paste(toupper(args[3]), "Air100x", sep=""), sep=""))

    results <- read.csv(res)
    x1      <- results$testParseWallTime
    x2      <- results$trainParseWallTime
    x3      <- results[paste(args[3],'BuildTime',sep="")][,1]
    x4      <- c(rbind(x1,x2,x3))
    cuts    <- cumsum(x4)
    cuts    <- cuts + min(MEM$time.s.)
    makeCuts(cuts,pID)
}

dev.off()
