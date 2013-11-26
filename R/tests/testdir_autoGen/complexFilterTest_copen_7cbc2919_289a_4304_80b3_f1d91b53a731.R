            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_copen_7cbc2919_289a_4304_80b3_f1d91b53a731 <- function(conn) {
                Log.info("A munge-task R unit test on data <copen> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading copen")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/princeton/copen.dat", "copen.hex")
            Log.info("Performing compound task ( hex[,c(\"n\")] != 54.8851325808 ) | ( hex[,c(\"n\")] < 64.1058272035 ) & ( ( hex[,c(\"n\")] < 73.8981572424 ))  on dataset <copen>")
                     filterHex <- hex[( hex[,c("n")] != 54.8851325808 ) | ( hex[,c("n")] < 64.1058272035 ) & ( ( hex[,c("n")] < 73.8981572424 )) ,]
            Log.info("Performing compound task ( hex[,c(\"n\")] != 78.7306773426 ) | ( hex[,c(\"n\")] < 44.6336180808 ) & ( ( hex[,c(\"n\")] < 43.0159480787 ))  on dataset copen, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("n")] != 78.7306773426 ) | ( hex[,c("n")] < 44.6336180808 ) & ( ( hex[,c("n")] < 43.0159480787 )) , c("n")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("n")] != 78.7306773426 ) | ( hex[,c("n")] < 44.6336180808 ) & ( ( hex[,c("n")] < 43.0159480787 )) , c("influence","housing","contact","satisfaction")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data copen", complexFilterTest_copen_7cbc2919_289a_4304_80b3_f1d91b53a731(conn)), error = function(e) FAIL(e))
            PASS()
