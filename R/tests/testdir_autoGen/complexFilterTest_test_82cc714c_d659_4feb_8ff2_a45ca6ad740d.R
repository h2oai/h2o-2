            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_82cc714c_d659_4feb_8ff2_a45ca6ad740d <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../../smalldata/chess/chess_2x2x100/R/test.csv", "test.hex")
            Log.info("Performing compound task !( ( ( hex[,c(\"y\")] != 0.452590132575 )) ) on dataset <test>")
                     filterHex <- hex[!( ( ( hex[,c("y")] != 0.452590132575 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(\"y\")] != 0.854400712064 )) ) on dataset test, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c("y")] != 0.854400712064 )) ), c("x","y")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c("y")] != 0.854400712064 )) ), c("color")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_82cc714c_d659_4feb_8ff2_a45ca6ad740d(conn)), error = function(e) FAIL(e))
            PASS()
