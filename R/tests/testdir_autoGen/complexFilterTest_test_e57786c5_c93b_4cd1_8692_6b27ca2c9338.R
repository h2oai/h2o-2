            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_e57786c5_c93b_4cd1_8692_6b27ca2c9338 <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../../smalldata/chess/chess_2x2x10/weka/test.csv.arff", "test.hex")
            Log.info("Performing compound task ( hex[,c(1)] != 1.234153138 ) | ( hex[,c(1)] < 0.993593405013 ) on dataset <test>")
                     filterHex <- hex[( hex[,c(1)] != 1.234153138 ) | ( hex[,c(1)] < 0.993593405013 ),]
            Log.info("Performing compound task ( hex[,c(1)] != 1.5166377041 ) | ( hex[,c(1)] < 1.33904133813 ) on dataset test, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 1.5166377041 ) | ( hex[,c(1)] < 1.33904133813 ), c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 1.5166377041 ) | ( hex[,c(1)] < 1.33904133813 ), c(2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_e57786c5_c93b_4cd1_8692_6b27ca2c9338(conn)), error = function(e) FAIL(e))
            PASS()
