            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_16b46fae_456a_4332_9fec_54bc478c7ada <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../../smalldata/chess/chess_2x2x10/weka/test.csv.arff", "test.hex")
            Log.info("Performing compound task !( ( hex[,c(1)] <= 1.79089826587 ) | ( hex[,c(1)] == 1.35887130758 ) & ( hex[,c(1)] <= 1.91610698089 ) & ( hex[,c(1)] <= 0.848997435424 ) & ( ( hex[,c(1)] <= 0.654634463113 )) ) on dataset <test>")
                     filterHex <- hex[!( ( hex[,c(1)] <= 1.79089826587 ) | ( hex[,c(1)] == 1.35887130758 ) & ( hex[,c(1)] <= 1.91610698089 ) & ( hex[,c(1)] <= 0.848997435424 ) & ( ( hex[,c(1)] <= 0.654634463113 )) ),]
            Log.info("Performing compound task !( ( hex[,c(1)] <= 0.6911381455 ) | ( hex[,c(1)] == 0.424451315706 ) & ( hex[,c(1)] <= 1.36651769606 ) & ( hex[,c(1)] <= 0.741838602461 ) & ( ( hex[,c(1)] <= 0.216526720125 )) ) on dataset test, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c(1)] <= 0.6911381455 ) | ( hex[,c(1)] == 0.424451315706 ) & ( hex[,c(1)] <= 1.36651769606 ) & ( hex[,c(1)] <= 0.741838602461 ) & ( ( hex[,c(1)] <= 0.216526720125 )) ), c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c(1)] <= 0.6911381455 ) | ( hex[,c(1)] == 0.424451315706 ) & ( hex[,c(1)] <= 1.36651769606 ) & ( hex[,c(1)] <= 0.741838602461 ) & ( ( hex[,c(1)] <= 0.216526720125 )) ), c(2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_16b46fae_456a_4332_9fec_54bc478c7ada(conn)), error = function(e) FAIL(e))
            PASS()
