            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_100kx7_logreg_b51d06ce_a22f_4961_a3f3_95263e7742ea <- function(conn) {
                Log.info("A munge-task R unit test on data <100kx7_logreg> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading 100kx7_logreg")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/100kx7_logreg.data.gz", "100kx7_logreg.hex")
            Log.info("Performing compound task !( ( ( hex[,c(7)] != 0.457564528337 )) ) on dataset <100kx7_logreg>")
                     filterHex <- hex[!( ( ( hex[,c(7)] != 0.457564528337 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(1)] != 5.49386791684 )) ) on dataset 100kx7_logreg, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 5.49386791684 )) ), c(1,3,2,5,4,7,6)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 5.49386791684 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data 100kx7_logreg", complexFilterTest_100kx7_logreg_b51d06ce_a22f_4961_a3f3_95263e7742ea(conn)), error = function(e) FAIL(e))
            PASS()
