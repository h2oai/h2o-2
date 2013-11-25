            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_test_extra_with_na_536e7532_1b98_44ba_b6ea_7f724871ff15 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris_test_extra_with_na> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading iris_test_extra_with_na")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/classifier/iris_test_extra_with_na.csv", "iris_test_extra_with_na.hex")
            Log.info("Performing compound task ( hex[,c(3)] != 0.159901462376 ) | ( hex[,c(1)] < 4.04768194651 ) & ( ( hex[,c(1)] < 3.52429117395 ))  on dataset <iris_test_extra_with_na>")
                     filterHex <- hex[( hex[,c(3)] != 0.159901462376 ) | ( hex[,c(1)] < 4.04768194651 ) & ( ( hex[,c(1)] < 3.52429117395 )) ,]
            Log.info("Performing compound task ( hex[,c(1)] != 4.185029059 ) | ( hex[,c(2)] < 6.50246173611 ) & ( ( hex[,c(1)] < 3.81317981647 ))  on dataset iris_test_extra_with_na, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 4.185029059 ) | ( hex[,c(2)] < 6.50246173611 ) & ( ( hex[,c(1)] < 3.81317981647 )) , c(1,3,2)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 4.185029059 ) | ( hex[,c(2)] < 6.50246173611 ) & ( ( hex[,c(1)] < 3.81317981647 )) , c(4)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris_test_extra_with_na", complexFilterTest_iris_test_extra_with_na_536e7532_1b98_44ba_b6ea_7f724871ff15(conn)), error = function(e) FAIL(e))
            PASS()
