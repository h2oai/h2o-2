            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_cb2063ce_62b2_4ea1_90c1_a0184a2a71aa <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x1000/weka/test.csv.arff", "test.hex")
            Log.info("Performing compound task !( ( hex[,c(1)] <= 0.905629834341 ) | ( hex[,c(1)] == 1.3605186493 ) & ( hex[,c(1)] <= 1.49750247211 ) & ( hex[,c(1)] <= 1.801858809 ) & ( ( hex[,c(1)] <= 1.28807827287 )) ) on dataset <test>")
                     filterHex <- hex[!( ( hex[,c(1)] <= 0.905629834341 ) | ( hex[,c(1)] == 1.3605186493 ) & ( hex[,c(1)] <= 1.49750247211 ) & ( hex[,c(1)] <= 1.801858809 ) & ( ( hex[,c(1)] <= 1.28807827287 )) ),]
            Log.info("Performing compound task !( ( hex[,c(1)] <= 0.464437509545 ) | ( hex[,c(1)] == 0.563152760019 ) & ( hex[,c(1)] <= 0.909140942953 ) & ( hex[,c(1)] <= 1.09034102486 ) & ( ( hex[,c(1)] <= 0.300955202792 )) ) on dataset test, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c(1)] <= 0.464437509545 ) | ( hex[,c(1)] == 0.563152760019 ) & ( hex[,c(1)] <= 0.909140942953 ) & ( hex[,c(1)] <= 1.09034102486 ) & ( ( hex[,c(1)] <= 0.300955202792 )) ), c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c(1)] <= 0.464437509545 ) | ( hex[,c(1)] == 0.563152760019 ) & ( hex[,c(1)] <= 0.909140942953 ) & ( hex[,c(1)] <= 1.09034102486 ) & ( ( hex[,c(1)] <= 0.300955202792 )) ), c(2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_cb2063ce_62b2_4ea1_90c1_a0184a2a71aa(conn)), error = function(e) FAIL(e))
            PASS()
