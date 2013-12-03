options(echo=FALSE)

##
# Utilities for relative paths in R
##

SEARCHPATH <- NULL
calcPath<-
function(path, root, optional_root_parent = NULL) {
    #takes the given path & root and searches upward...
    #if not found, searches R/*
    #use optional_root_parent if your root directory is too generic (e.g. root = "tests")
    bdp <- basename(dirname(path)) == root
    rddNbdpEopr <- root %in% dir(dirname(path)) & (is.null(optional_root_parent) || basename(dirname(path)) == optional_root_parent)

    if (basename(path) == root || root %in% dir(path)) {  #rddNbdpEopr) {
        return(0)
    }

    if(!is.null(optional_root_parent)) cat("Using optional root parent: ", optional_root_parent)
    if (basename(path) == "h2o" || "smalldata" %in% dir(path)) {
        print("[INFO]: Could not find the bucket that you specified! Checking R/*. Will fail if cannot find")
        SEARCHPATH <<- path
        return(-1)
    }
    if ( bdp || rddNbdpEopr ) {
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
function(dataName = NULL, bucket = NULL, path = NULL, fullPath = NULL, schema = "put") {
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
            distance.bucket.root <- calcPath(getwd(), bucket)
            if (distance.bucket.root < 0) {
                Log.err(paste("Could not find bucket ", bucket, "\n"))
            }
            bucket.dots <- genDots(distance.bucket.root)
            fullPath <- paste(bucket.dots,bucket,'/',path,sep="")
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
        Log.info(cat("BUCKET: ", bucket, " PATH: ", path, " SCHEMA: ", schema))
        return(locate(bucket = bucket, path = path, schema = schema))
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
    source(paste(path, "h2oRClient-package/R/Algorithms.R", sep = ""))
    source(paste(path, "h2oRClient-package/R/Classes.R", sep = ""))
    source(paste(path, "h2oRClient-package/R/ParseImport.R", sep = ""))
    source(paste(path, "h2oRClient-package/R/Internal.R", sep = ""))
    sandbox()
} else {
    distance <- calcPath(getwd(), "tests")
    dots     <- genDots(distance)
    newPath  <- paste(dots, "Utils/h2oR.R", sep = "")
    source(newPath)

    #rdots is the calculated path to the R source files...
    rdots <- ifelse(dots == "./", "../", paste("../", dots, sep = ""))

    source(paste(rdots, "h2oRClient-package/R/Algorithms.R", sep = ""))
    source(paste(rdots, "h2oRClient-package/R/Classes.R", sep = ""))
    source(paste(rdots, "h2oRClient-package/R/ParseImport.R", sep = ""))
    source(paste(rdots, "h2oRClient-package/R/Internal.R", sep = ""))
    sandbox()
}
