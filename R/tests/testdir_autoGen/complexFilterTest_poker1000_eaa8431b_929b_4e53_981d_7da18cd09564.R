            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_poker1000_eaa8431b_929b_4e53_981d_7da18cd09564 <- function(conn) {
                Log.info("A munge-task R unit test on data <poker1000> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading poker1000")
                hex <- h2o.uploadFile(conn, "../../../smalldata/poker/poker1000", "poker1000.hex")
            Log.info("Performing compound task ( hex[,c(1)] != 1.0 ) | ( hex[,c(1)] == 1.0 ) & ( hex[,c(8)] !| 1.26457044282 ) ! ( hex[,c(8)] > 2.42847821807 ) on dataset <poker1000>")
                     filterHex <- hex[( hex[,c(1)] != 1.0 ) | ( hex[,c(1)] == 1.0 ) & ( hex[,c(8)] !| 1.26457044282 ) ! ( hex[,c(8)] > 2.42847821807 ),]
            Log.info("Performing compound task ( hex[,c(10)] != 5.01433591014 ) | ( hex[,c(4)] == 1.0 ) & ( hex[,c(3)] !| 2.0 ) ! ( hex[,c(8)] > 3.42696074482 ) on dataset poker1000, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(10)] != 5.01433591014 ) | ( hex[,c(4)] == 1.0 ) & ( hex[,c(3)] !| 2.0 ) ! ( hex[,c(8)] > 3.42696074482 ), c(10,1,3,2,5,4,7,6,9,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(10)] != 5.01433591014 ) | ( hex[,c(4)] == 1.0 ) & ( hex[,c(3)] !| 2.0 ) ! ( hex[,c(8)] > 3.42696074482 ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data poker1000", complexFilterTest_poker1000_eaa8431b_929b_4e53_981d_7da18cd09564(conn)), error = function(e) FAIL(e))
            PASS()
