            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_ac8f17ea_d6b0_40a5_8d69_f0f439820ff3 <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x1000/R/test.csv", "test.hex")
            Log.info("Performing compound task ( hex[,c(\"x\")] != 1.3279212853 ) | ( hex[,c(\"y\")] == 0.705261446705 ) on dataset <test>")
                     filterHex <- hex[( hex[,c("x")] != 1.3279212853 ) | ( hex[,c("y")] == 0.705261446705 ),]
            Log.info("Performing compound task ( hex[,c(\"y\")] != 0.768083544158 ) | ( hex[,c(\"y\")] == 0.16407319585 ) on dataset test, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("y")] != 0.768083544158 ) | ( hex[,c("y")] == 0.16407319585 ), c("y")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("y")] != 0.768083544158 ) | ( hex[,c("y")] == 0.16407319585 ), c("x","color")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_ac8f17ea_d6b0_40a5_8d69_f0f439820ff3(conn)), error = function(e) FAIL(e))
            PASS()
