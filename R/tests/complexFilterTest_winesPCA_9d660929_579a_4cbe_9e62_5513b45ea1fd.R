            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_winesPCA_9d660929_579a_4cbe_9e62_5513b45ea1fd <- function(conn) {
                Log.info("A munge-task R unit test on data <winesPCA> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading winesPCA")
                hex <- h2o.uploadFile(conn, "../../smalldata/winesPCA.csv", "winesPCA.hex")
            Log.info("Performing compound task ( hex[,c(1)] != -2.24018717383 ) | ( hex[,c(1)] < -1.04134952647 ) & ( ( hex[,c(1)] < -2.12199903709 ))  on dataset <winesPCA>")
                     filterHex <- hex[( hex[,c(1)] != -2.24018717383 ) | ( hex[,c(1)] < -1.04134952647 ) & ( ( hex[,c(1)] < -2.12199903709 )) ,]
            Log.info("Performing compound task ( hex[,c(1)] != 0.786274498953 ) | ( hex[,c(1)] < -1.9483202097 ) & ( ( hex[,c(1)] < 1.95248656742 ))  on dataset winesPCA, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.786274498953 ) | ( hex[,c(1)] < -1.9483202097 ) & ( ( hex[,c(1)] < 1.95248656742 )) , c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.786274498953 ) | ( hex[,c(1)] < -1.9483202097 ) & ( ( hex[,c(1)] < 1.95248656742 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data winesPCA", complexFilterTest_winesPCA_9d660929_579a_4cbe_9e62_5513b45ea1fd(conn)), error = function(e) FAIL(e))
            PASS()
