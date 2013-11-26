            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_3c0fb217_8390_47c4_83e9_b076924f59e2 <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../../smalldata/chess/chess_2x2x500/weka/test.csv.arff", "test.hex")
            Log.info("Performing compound task ( hex[,c(1)] != 1.99518387641 ) | ( hex[,c(1)] < 0.457178736507 ) & ( ( hex[,c(1)] < 0.275477288342 ))  on dataset <test>")
                     filterHex <- hex[( hex[,c(1)] != 1.99518387641 ) | ( hex[,c(1)] < 0.457178736507 ) & ( ( hex[,c(1)] < 0.275477288342 )) ,]
            Log.info("Performing compound task ( hex[,c(1)] != 0.718979349684 ) | ( hex[,c(1)] < 0.687071110828 ) & ( ( hex[,c(1)] < 1.06129768553 ))  on dataset test, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.718979349684 ) | ( hex[,c(1)] < 0.687071110828 ) & ( ( hex[,c(1)] < 1.06129768553 )) , c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 0.718979349684 ) | ( hex[,c(1)] < 0.687071110828 ) & ( ( hex[,c(1)] < 1.06129768553 )) , c(2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_3c0fb217_8390_47c4_83e9_b076924f59e2(conn)), error = function(e) FAIL(e))
            PASS()
