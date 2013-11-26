            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_HTWO-87-two-unique-lines-dataset_0a7718d3_68fd_47ad_838a_7dd1907c9394 <- function(conn) {
                Log.info("A munge-task R unit test on data <HTWO-87-two-unique-lines-dataset> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading HTWO-87-two-unique-lines-dataset")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/HTWO-87-two-unique-lines-dataset.csv", "HTWO-87-two-unique-lines-dataset.hex")
            Log.info("Performing compound task ( hex[,c(5)] != 0.0 ) | ( hex[,c(8)] == 0.201214387806 ) & ( hex[,c(2)] !| 0.0 ) ! ( hex[,c(1)] > 0.0 ) on dataset <HTWO-87-two-unique-lines-dataset>")
                     filterHex <- hex[( hex[,c(5)] != 0.0 ) | ( hex[,c(8)] == 0.201214387806 ) & ( hex[,c(2)] !| 0.0 ) ! ( hex[,c(1)] > 0.0 ),]
            Log.info("Performing compound task ( hex[,c(1)] != 0.0 ) | ( hex[,c(1)] == 0.0 ) & ( hex[,c(3)] !| 5.0 ) ! ( hex[,c(6)] > 0.0 ) on dataset HTWO-87-two-unique-lines-dataset, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.0 ) | ( hex[,c(1)] == 0.0 ) & ( hex[,c(3)] !| 5.0 ) ! ( hex[,c(6)] > 0.0 ), c(1,3,5,4,7,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.0 ) | ( hex[,c(1)] == 0.0 ) & ( hex[,c(3)] !| 5.0 ) ! ( hex[,c(6)] > 0.0 ), c(2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data HTWO-87-two-unique-lines-dataset", complexFilterTest_HTWO-87-two-unique-lines-dataset_0a7718d3_68fd_47ad_838a_7dd1907c9394(conn)), error = function(e) FAIL(e))
            PASS()
