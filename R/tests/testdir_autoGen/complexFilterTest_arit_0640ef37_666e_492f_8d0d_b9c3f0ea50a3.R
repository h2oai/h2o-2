            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_arit_0640ef37_666e_492f_8d0d_b9c3f0ea50a3 <- function(conn) {
                Log.info("A munge-task R unit test on data <arit> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading arit")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/arit.csv", "arit.hex")
            Log.info("Performing compound task !( ( ( hex[,c(1)] != 434392.292205 )) ) on dataset <arit>")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 434392.292205 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(1)] != 912055.553811 )) ) on dataset arit, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 912055.553811 )) ), c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 912055.553811 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data arit", complexFilterTest_arit_0640ef37_666e_492f_8d0d_b9c3f0ea50a3(conn)), error = function(e) FAIL(e))
            PASS()
