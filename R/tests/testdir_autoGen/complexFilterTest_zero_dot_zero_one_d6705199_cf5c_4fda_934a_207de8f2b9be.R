            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_zero_dot_zero_one_d6705199_cf5c_4fda_934a_207de8f2b9be <- function(conn) {
                Log.info("A munge-task R unit test on data <zero_dot_zero_one> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading zero_dot_zero_one")
                hex <- h2o.uploadFile(conn, "../../../smalldata/zero_dot_zero_one.csv", "zero_dot_zero_one.hex")
            Log.info("Performing compound task ( hex[,c(1)] != 0.01 ) | ( hex[,c(1)] < 0.01 ) & ( ( hex[,c(1)] < 0.01 ))  on dataset <zero_dot_zero_one>")
                     filterHex <- hex[( hex[,c(1)] != 0.01 ) | ( hex[,c(1)] < 0.01 ) & ( ( hex[,c(1)] < 0.01 )) ,]
            Log.info("Performing compound task ( hex[,c(1)] != 0.01 ) | ( hex[,c(1)] < 0.01 ) & ( ( hex[,c(1)] < 0.01 ))  on dataset zero_dot_zero_one, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.01 ) | ( hex[,c(1)] < 0.01 ) & ( ( hex[,c(1)] < 0.01 )) , c(1,2)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.01 ) | ( hex[,c(1)] < 0.01 ) & ( ( hex[,c(1)] < 0.01 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data zero_dot_zero_one", complexFilterTest_zero_dot_zero_one_d6705199_cf5c_4fda_934a_207de8f2b9be(conn)), error = function(e) FAIL(e))
            PASS()
