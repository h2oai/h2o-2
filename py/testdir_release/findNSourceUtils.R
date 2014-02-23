options(echo=FALSE)

##
# Utilities for relative paths in R
##

SEARCHPATH <- NULL
ROOTISPARENT <- FALSE
calcPath<-
function(path, root, optional_root_parent = NULL) {
    #takes the given path & root and searches upward...
    #if not found, searches R/*
    #use optional_root_parent if your root directory is too generic (e.g. root = "tests")
    bdp <- basename(dirname(path)) == root
    rddNbdpEopr <- root %in% dir(dirname(path)) & (is.null(optional_root_parent) || basename(dirname(path)) == optional_root_parent)

    if (basename(path) == root || root %in% dir(path)) {  #rddNbdpEopr) {
        #I am in the root being searched for, or it's in the directory I am in currently
        if(!is.null(optional_root_parent)) ROOTISPARENT <<- TRUE
        return(0)
    }

    if(!is.null(optional_root_parent)) cat("\nUsing optional root parent: ", optional_root_parent, "\n")
    if (basename(path) == "h2o" || "smalldata" %in% dir(path)) {
        print("\n[INFO]: Could not find the bucket that you specified! Checking R/*. Will fail if cannot find\n")
        SEARCHPATH <<- path
        return(-1)
    }
    if ( bdp || rddNbdpEopr ) {
        #The root is in the directory parent to my current directory, and the parent of my parent is null or the optional_root_parent
        #print("\n[INFO]: ROOT is in the parent of current directory!")  
        if(!is.null(optional_root_parent)) ROOTISPARENT <<- TRUE
        return(1)
    }
    return(ifelse( calcPath( dirname(path), root, optional_root_parent) < 0, -1, 1 + calcPath( dirname(path), root, optional_root_parent) ) )
}

genDots<-
function(distance) {
    if(distance == 0) return('./')
    return(paste(rep("../", distance), collapse=""))
}

locate<-
function(dataName = NULL, bucket = NULL, path = NULL, fullPath = NULL, schema = "put", optional_root_parent = NULL) {
    if (!is.null(fullPath)) {   
        if (schema == "local") return(paste("./", gsub("[./]",fullPath), sep = ""))
        return(fullPath) #schema is put
    }

    if(!is.null(bucket)) {
        if(is.null(path)) stop("\"path\" must be specified along with bucket. Path is the bucket offset.")
        bucket <- gsub("[./]","",bucket)
        path   <- ifelse(substring(path,1,1) == '/', substring(path,2), path)
        path   <- ifelse(substring(path,nchar(path)) == '/', substring(path,1,nchar(path)-1),path)
        if (schema == "local") return(paste("./",bucket,"/",path,sep = ""))
        if (schema == "put") {
            distance.bucket.root <- calcPath(getwd(), bucket, optional_root_parent)
            if (distance.bucket.root < 0 && optional_root_parent == "R") {
                fullPath <- paste(SEARCHPATH, '/', optional_root_parent, '/', bucket, '/', path, sep = "", collapse = "")
                return(fullPath)
            }
            if (distance.bucket.root < 0 && is.null(optional_root_parent)) {
                Log.err(paste("Could not find bucket <", bucket, ">\n"))
            }
            bucket.dots <- genDots(distance.bucket.root)
            if (is.null(optional_root_parent)) ROOTISPARENT <<- FALSE
            fullPath <- ifelse(ROOTISPARENT == TRUE, paste(bucket.dots,path,sep = ""), paste(bucket.dots,bucket,'/',path,sep=""))
            return(fullPath)
        }
        if (schema == "S3") stop("Unimpl")
    }

    if (!is.null(dataName)) {
        bn <- basename(dataName)
        dataName <- dirname(dataName)
        dataName <- gsub("\\.","", gsub("\\./","",dataName))
        if(!is.null(SEARCHPATH)) return(paste(SEARCHPATH, "/", dataName, "/", bn, sep = ""))
        psplit <- strsplit(dataName, "/")[[1]]
        bucket <- psplit[1]
        path   <- paste(psplit[-1], collapse="/")
        path   <- paste(path, bn, sep = "/")
        m <- paste("BUCKET: ", bucket, " PATH: ", path, " SCHEMA: ", schema)
        Log.info(m)
        return(locate(bucket = bucket, path = path, schema = schema))
    }
}

select.help<-
function() {
    datajson <- locate(bucket = "tests", path = "Utils/smalldata.json", optional_root_parent = "R")
    datajson <- fromJSON(paste(readLines(datajson), collapse=""))
    random.dataset.id <- sample(length(datajson$datasets),1)
    ROOTISPARENT <<- FALSE
    return(datajson$datasets[random.dataset.id][[1]])
}

select<-
function() {
    a <- select.help()
    if ( a[[1]]$ATTRS$NUMROWS != "0" && a[[1]]$ATTRS$NUMCOLS != "0" && file_test("-f", locate(a[[1]]$PATHS[1]))) {
        return(a)
    } else {
        return(select())
    }
}

getBucket<-
function(bucket = NULL) {
    if(is.null(bucket)) stop("Did not specify bucket...")
    print(bucket)
    bucket <- gsub("[./]","",bucket)
    distance.bucket.root <- calcPath(getwd(), bucket)
    bucket.dots <- genDots(distance.bucket.root)
    newBucket <- paste(bucket.dots, bucket, sep  ="")
    return(newBucket)
}

distance <- calcPath(getwd(), "tests", "R")
if (distance < 0) {
    path <- paste(SEARCHPATH, "/R/", sep = "")
    source(paste(path, "tests/Utils/h2oR.R", sep = ""))
    source(paste(path, "tests/Utils/pcaR.R", sep = ""))
    source(paste(path, "tests/Utils/glmR.R", sep = ""))
    source(paste(path, "tests/Utils/gbmR.R", sep = ""))
    source(paste(path, "tests/Utils/utilsR.R", sep = ""))
    source(paste(path, "h2o-package/R/Algorithms.R", sep = ""))
    source(paste(path, "h2o-package/R/Classes.R", sep = ""))
    source(paste(path, "h2o-package/R/ParseImport.R", sep = ""))
    source(paste(path, "h2o-package/R/Internal.R", sep = ""))
} else {
    distance <- calcPath(getwd(), "tests")
    dots     <- genDots(distance)
    source(paste(dots, "Utils/h2oR.R", sep = ""))
    source(paste(dots, "Utils/pcaR.R", sep = ""))
    source(paste(dots, "Utils/glmR.R", sep = ""))
    source(paste(dots, "Utils/gbmR.R", sep = ""))
    source(paste(dots, "Utils/utilsR.R", sep = ""))

    #rdots is the calculated path to the R source files...
    rdots <- ifelse(dots == "./", "../", paste("../", dots, sep = ""))

    source(paste(rdots, "h2o-package/R/Algorithms.R", sep = ""))
    source(paste(rdots, "h2o-package/R/Classes.R", sep = ""))
    source(paste(rdots, "h2o-package/R/ParseImport.R", sep = ""))
    source(paste(rdots, "h2o-package/R/Internal.R", sep = ""))
}
sandbox()
#This random seed is overwritten by any seed set in a test
setupRandomSeed(suppress = TRUE)
h2o.removeAll(new("H2OClient", ip=myIP, port=myPort))
