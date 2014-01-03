genString<-
function(i = NULL) {
    res <- paste(sample(letters, 26, replace = TRUE), collapse = "", sep = "")
    return(res)
}

setupRandomSeed<-
function(seed = NULL, suppress = FALSE) {
    test_name <- R.utils::commandArgs(asValues=TRUE)$"-f"
    possible_seed_path <- paste("./Rsandbox_", test_name, "/seed", sep = "")
    if (file.exists(possible_seed_path)) {
        fileseed <- read.table(possible_seed_path)[[1]]
        SEED <<- fileseed
        set.seed(fileseed)
        return(fileseed)
    }
    if (MASTER_SEED) {
        #SEED <<- seed
        cat("\n\n\n", paste("[INFO]: Using master SEED to generate a new seed for this test: ", seed), "\n\n\n\n")
        h2o.__logIt("[Master-SEED] :", seed, "Command")
        maxInt <- .Machine$integer.max
        newseed <- sample(maxInt, 1)
        cat("\n\n\n", paste("[INFO]: Using seed for this test ", newseed), "\n\n\n\n")
        SEED <<- newseed
        set.seed(newseed)
        return(newseed)
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

