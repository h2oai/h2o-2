            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_poker1000_c6427861_89ff_4b96_aa97_fa7b6fce2127 <- function(conn) {
                Log.info("A munge-task R unit test on data <poker1000> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading poker1000")
                hex <- h2o.uploadFile(conn, "../../../smalldata/poker/poker1000", "poker1000.hex")
            Log.info("Performing compound task !( ( ( hex[,c(10)] != 0.385419926555 )) ) on dataset <poker1000>")
                     filterHex <- hex[!( ( ( hex[,c(10)] != 0.385419926555 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(3)] != 2.0 )) ) on dataset poker1000, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(3)] != 2.0 )) ), c(10,1,3,2,5,4,7,6,9,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(3)] != 2.0 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data poker1000", complexFilterTest_poker1000_c6427861_89ff_4b96_aa97_fa7b6fce2127(conn)), error = function(e) FAIL(e))
            PASS()
