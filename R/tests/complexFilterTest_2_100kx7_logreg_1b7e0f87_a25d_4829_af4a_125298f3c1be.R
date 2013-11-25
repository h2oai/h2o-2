            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_2_100kx7_logreg_1b7e0f87_a25d_4829_af4a_125298f3c1be <- function(conn) {
                Log.info("A munge-task R unit test on data <2_100kx7_logreg> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading 2_100kx7_logreg")
                hex <- h2o.uploadFile(conn, "../../smalldata/2_100kx7_logreg.data.gz", "2_100kx7_logreg.hex")
            Log.info("Performing compound task !( ( ( hex[,c(6)] != 483.858800578 )) ) on dataset <2_100kx7_logreg>")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 483.858800578 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(6)] != 83.3938048129 )) ) on dataset 2_100kx7_logreg, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 83.3938048129 )) ), c(1,3,2,5,4,7,6)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 83.3938048129 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data 2_100kx7_logreg", complexFilterTest_2_100kx7_logreg_1b7e0f87_a25d_4829_af4a_125298f3c1be(conn)), error = function(e) FAIL(e))
            PASS()
