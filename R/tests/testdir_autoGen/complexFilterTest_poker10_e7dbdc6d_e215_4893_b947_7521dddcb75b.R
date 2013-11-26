            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_poker10_e7dbdc6d_e215_4893_b947_7521dddcb75b <- function(conn) {
                Log.info("A munge-task R unit test on data <poker10> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading poker10")
                hex <- h2o.uploadFile(conn, "../../../smalldata/poker/poker10", "poker10.hex")
            Log.info("Performing compound task ( hex[,c(4)] != 1.0 ) | ( hex[,c(1)] < 1.0 ) & ( ( hex[,c(8)] < 1.0 ))  on dataset <poker10>")
                     filterHex <- hex[( hex[,c(4)] != 1.0 ) | ( hex[,c(1)] < 1.0 ) & ( ( hex[,c(8)] < 1.0 )) ,]
            Log.info("Performing compound task ( hex[,c(9)] != 9.37835664187 ) | ( hex[,c(6)] < 1.0 ) & ( ( hex[,c(1)] < 1.0 ))  on dataset poker10, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(9)] != 9.37835664187 ) | ( hex[,c(6)] < 1.0 ) & ( ( hex[,c(1)] < 1.0 )) , c(10,1,2,5,4,7,6,9)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(9)] != 9.37835664187 ) | ( hex[,c(6)] < 1.0 ) & ( ( hex[,c(1)] < 1.0 )) , c(8,3)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data poker10", complexFilterTest_poker10_e7dbdc6d_e215_4893_b947_7521dddcb75b(conn)), error = function(e) FAIL(e))
            PASS()
