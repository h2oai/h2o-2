            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_HTWO-87-one-line-dataset-2unix_63165766_9303_43f8_bee8_e71784da62a0 <- function(conn) {
                Log.info("A munge-task R unit test on data <HTWO-87-one-line-dataset-2unix> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading HTWO-87-one-line-dataset-2unix")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/HTWO-87-one-line-dataset-2unix.csv", "HTWO-87-one-line-dataset-2unix.hex")
            Log.info("Performing compound task ( hex[,c(5)] != 0.0 ) | ( hex[,c(1)] == 0.0 ) & ( hex[,c(7)] !| 5.0 ) ! ( hex[,c(1)] > 10.0 ) on dataset <HTWO-87-one-line-dataset-2unix>")
                     filterHex <- hex[( hex[,c(5)] != 0.0 ) | ( hex[,c(1)] == 0.0 ) & ( hex[,c(7)] !| 5.0 ) ! ( hex[,c(1)] > 10.0 ),]
            Log.info("Performing compound task ( hex[,c(3)] != 5.0 ) | ( hex[,c(1)] == 10.0 ) & ( hex[,c(1)] !| 0.0 ) ! ( hex[,c(1)] > 0.0 ) on dataset HTWO-87-one-line-dataset-2unix, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(3)] != 5.0 ) | ( hex[,c(1)] == 10.0 ) & ( hex[,c(1)] !| 0.0 ) ! ( hex[,c(1)] > 0.0 ), c(1,3,2,5,4,7,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(3)] != 5.0 ) | ( hex[,c(1)] == 10.0 ) & ( hex[,c(1)] !| 0.0 ) ! ( hex[,c(1)] > 0.0 ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data HTWO-87-one-line-dataset-2unix", complexFilterTest_HTWO-87-one-line-dataset-2unix_63165766_9303_43f8_bee8_e71784da62a0(conn)), error = function(e) FAIL(e))
            PASS()
