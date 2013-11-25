            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_389204ff_ad08_40ce_bc3a_15b4b36810b1 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading iris")
                hex <- h2o.uploadFile(conn, "../../smalldata/iris/iris.csv", "iris.hex")
            Log.info("Performing compound task ( hex[,c(2)] != 4.50643839893 ) | ( hex[,c(3)] < 1.70269812646 ) & ( ( hex[,c(1)] < 2.45804933394 ))  on dataset <iris>")
                     filterHex <- hex[( hex[,c(2)] != 4.50643839893 ) | ( hex[,c(3)] < 1.70269812646 ) & ( ( hex[,c(1)] < 2.45804933394 )) ,]
            Log.info("Performing compound task ( hex[,c(2)] != 6.84731367821 ) | ( hex[,c(3)] < 0.627278163035 ) & ( ( hex[,c(2)] < 5.19528191323 ))  on dataset iris, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(2)] != 6.84731367821 ) | ( hex[,c(3)] < 0.627278163035 ) & ( ( hex[,c(2)] < 5.19528191323 )) , c(1,3,2)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(2)] != 6.84731367821 ) | ( hex[,c(3)] < 0.627278163035 ) & ( ( hex[,c(2)] < 5.19528191323 )) , c(4)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris", complexFilterTest_iris_389204ff_ad08_40ce_bc3a_15b4b36810b1(conn)), error = function(e) FAIL(e))
            PASS()
