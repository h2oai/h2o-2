            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_prostate_3_b0e7955f_b7a8_41bc_a191_3b9c703a4d26 <- function(conn) {
                Log.info("A munge-task R unit test on data <prostate_3> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading prostate_3")
                hex <- h2o.uploadFile(conn, "../../smalldata/parse_folder_test/prostate_3.csv", "prostate_3.hex")
            Log.info("Performing compound task !( ( ( hex[,c(6)] != 15.4384729024 )) ) on dataset <prostate_3>")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 15.4384729024 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(6)] != 38.0343090854 )) ) on dataset prostate_3, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 38.0343090854 )) ), c(1,2,5,4,7,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(6)] != 38.0343090854 )) ), c(3)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data prostate_3", complexFilterTest_prostate_3_b0e7955f_b7a8_41bc_a191_3b9c703a4d26(conn)), error = function(e) FAIL(e))
            PASS()
