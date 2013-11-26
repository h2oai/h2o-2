            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_caba88b5_c6d0_4508_981f_aeb8e0b0d83d <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../../smalldata/chess/chess_2x2x100/R/test.csv", "test.hex")
            Log.info("Performing compound task !( ( hex[,c(\"y\")] <= 0.311485755605 ) | ( hex[,c(\"x\")] == 0.289892999758 ) & ( hex[,c(\"y\")] <= 1.18994397443 ) & ( hex[,c(\"x\")] <= 0.24814766038 ) & ( ( hex[,c(\"x\")] <= 1.54630098994 )) ) on dataset <test>")
                     filterHex <- hex[!( ( hex[,c("y")] <= 0.311485755605 ) | ( hex[,c("x")] == 0.289892999758 ) & ( hex[,c("y")] <= 1.18994397443 ) & ( hex[,c("x")] <= 0.24814766038 ) & ( ( hex[,c("x")] <= 1.54630098994 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"y\")] <= 0.681854704834 ) | ( hex[,c(\"y\")] == 0.178026868362 ) & ( hex[,c(\"x\")] <= 0.18699934396 ) & ( hex[,c(\"y\")] <= 1.92997343946 ) & ( ( hex[,c(\"x\")] <= 1.75450107676 )) ) on dataset test, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("y")] <= 0.681854704834 ) | ( hex[,c("y")] == 0.178026868362 ) & ( hex[,c("x")] <= 0.18699934396 ) & ( hex[,c("y")] <= 1.92997343946 ) & ( ( hex[,c("x")] <= 1.75450107676 )) ), c("x","y")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("y")] <= 0.681854704834 ) | ( hex[,c("y")] == 0.178026868362 ) & ( hex[,c("x")] <= 0.18699934396 ) & ( hex[,c("y")] <= 1.92997343946 ) & ( ( hex[,c("x")] <= 1.75450107676 )) ), c("color")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_caba88b5_c6d0_4508_981f_aeb8e0b0d83d(conn)), error = function(e) FAIL(e))
            PASS()
