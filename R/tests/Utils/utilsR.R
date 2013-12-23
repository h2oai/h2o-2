genString<-
function(i = NULL) {
    res <- paste(sample(letters, 26, replace = TRUE), collapse = "", sep = "")
    return(res)
}

setupRandomSeed<-
function(seed = NULL, suppress = FALSE) {
    if (MASTER_SEED) {
        SEED <<- seed
        cat("\n\n\n", paste("[INFO]: Using master SEED: ", seed), "\n\n\n\n")
        h2o.__logIt("[Master-SEED] :", seed, "Command")
        set.seed(seed)
        return(seed)
    }
    if (!is.null(seed)) {
        SEED <<- seed
        set.seed(seed)
        cat("\n\n\n", paste("[INFO]: Using user defined SEED: ", seed), "\n\n\n\n")
        h2o.__logIt("[User-SEED] :", seed, "Command")
        return(seed)
    } else {
        maxInt <- .Machine$integer.max
        seed <- sample(maxInt, 1)
        SEED <<- seed
        if(!suppress) {
          cat("\n\n\n", paste("[INFO]: Using SEED: ", seed), "\n\n\n\n")
          h2o.__logIt("[SEED] :", seed, "Command")
        }
        set.seed(seed)
        return(seed)
    }
    Log.info(paste("USING SEED: ", SEED))
}

