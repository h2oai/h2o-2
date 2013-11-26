            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_pharynx_60619f14_12e7_4c1c_8c0b_f28134011654 <- function(conn) {
                Log.info("A munge-task R unit test on data <pharynx> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading pharynx")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/umass_statdata/pharynx.dat", "pharynx.hex")
            Log.info("Performing compound task !( ( ( hex[,c(11)] != 0.6443474172 )) ) on dataset <pharynx>")
                     filterHex <- hex[!( ( ( hex[,c(11)] != 0.6443474172 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(6)] != 6.8931175114 )) ) on dataset pharynx, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 6.8931175114 )) ), c(2,4,6)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 6.8931175114 )) ), c(11,10,12,1,3,5,7,9,8)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data pharynx", complexFilterTest_pharynx_60619f14_12e7_4c1c_8c0b_f28134011654(conn)), error = function(e) FAIL(e))
            PASS()
