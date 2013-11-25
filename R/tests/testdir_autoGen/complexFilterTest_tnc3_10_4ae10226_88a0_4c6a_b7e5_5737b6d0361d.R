            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_tnc3_10_4ae10226_88a0_4c6a_b7e5_5737b6d0361d <- function(conn) {
                Log.info("A munge-task R unit test on data <tnc3_10> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading tnc3_10")
                hex <- h2o.uploadFile(conn, "../../../smalldata/tnc3_10.csv", "tnc3_10.hex")
            Log.info("Performing compound task !( ( hex[,c(\"pclass\")] <= 1.0 ) | ( hex[,c(\"fare\")] == 48.3666316847 ) & ( hex[,c(\"fare\")] <= 76.9862211755 ) & ( hex[,c(\"survived\")] <= 0.21373415302 ) & ( ( hex[,c(\"parch\")] <= 1.86376769712 )) ) on dataset <tnc3_10>")
                     filterHex <- hex[!( ( hex[,c("pclass")] <= 1.0 ) | ( hex[,c("fare")] == 48.3666316847 ) & ( hex[,c("fare")] <= 76.9862211755 ) & ( hex[,c("survived")] <= 0.21373415302 ) & ( ( hex[,c("parch")] <= 1.86376769712 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"age\")] <= 30.1087146513 ) | ( hex[,c(\"pclass\")] == 1.0 ) & ( hex[,c(\"fare\")] <= 40.264163379 ) & ( hex[,c(\"survived\")] <= 0.769079541209 ) & ( ( hex[,c(\"sibsp\")] <= 1.39367434774 )) ) on dataset tnc3_10, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("age")] <= 30.1087146513 ) | ( hex[,c("pclass")] == 1.0 ) & ( hex[,c("fare")] <= 40.264163379 ) & ( hex[,c("survived")] <= 0.769079541209 ) & ( ( hex[,c("sibsp")] <= 1.39367434774 )) ), c("fare","pclass","survived","age","sibsp","parch")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("age")] <= 30.1087146513 ) | ( hex[,c("pclass")] == 1.0 ) & ( hex[,c("fare")] <= 40.264163379 ) & ( hex[,c("survived")] <= 0.769079541209 ) & ( ( hex[,c("sibsp")] <= 1.39367434774 )) ), c("boat","name","sex","ticket","cabin","home.dest","body","embarked")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data tnc3_10", complexFilterTest_tnc3_10_4ae10226_88a0_4c6a_b7e5_5737b6d0361d(conn)), error = function(e) FAIL(e))
            PASS()
