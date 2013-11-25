            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_dd-mon-yy-with-other-cols_bfabd23d_c11e_4310_9c97_dfc928183f0c <- function(conn) {
                Log.info("A munge-task R unit test on data <dd-mon-yy-with-other-cols> testing the functional unit <>=> ")
                Log.info("Uploading dd-mon-yy-with-other-cols")
                hex <- h2o.uploadFile(conn, "../../smalldata/datetime/dd-mon-yy-with-other-cols.csv", "dd-mon-yy-with-other-cols.hex")
                Log.info("Filtering out rows by >= from dataset dd-mon-yy-with-other-cols and column \"0\" using value 6716.64789001")
                     filterHex <- hex[hex[,c(1)] >= 6716.64789001,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 6716.64789001,]
                    Log.info("Filtering out rows by >= from dataset dd-mon-yy-with-other-cols and column \"0\" using value 6543.61631089, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 6543.61631089, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 6543.61631089, c(1,2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data dd-mon-yy-with-other-cols", simpleFilterTest_dd-mon-yy-with-other-cols_bfabd23d_c11e_4310_9c97_dfc928183f0c(conn)), error = function(e) FAIL(e))
            PASS()
