            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_train_639cff3d_4591_4237_a5b7_b091b4f20e4b <- function(conn) {
                Log.info("A munge-task R unit test on data <train> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading train")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x1000/R/train.csv", "train.hex")
            Log.info("Performing compound task ( hex[,c(\"y\")] != 0.495288169615 ) | ( hex[,c(\"y\")] < 1.78329114683 ) & ( ( hex[,c(\"y\")] < 0.2314068932 ))  on dataset <train>")
                     filterHex <- hex[( hex[,c("y")] != 0.495288169615 ) | ( hex[,c("y")] < 1.78329114683 ) & ( ( hex[,c("y")] < 0.2314068932 )) ,]
            Log.info("Performing compound task ( hex[,c(\"x\")] != 0.1368402839 ) | ( hex[,c(\"y\")] < 0.0405012437564 ) & ( ( hex[,c(\"y\")] < 0.663257058033 ))  on dataset train, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("x")] != 0.1368402839 ) | ( hex[,c("y")] < 0.0405012437564 ) & ( ( hex[,c("y")] < 0.663257058033 )) , c("x","y")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("x")] != 0.1368402839 ) | ( hex[,c("y")] < 0.0405012437564 ) & ( ( hex[,c("y")] < 0.663257058033 )) , c("color")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data train", complexFilterTest_train_639cff3d_4591_4237_a5b7_b091b4f20e4b(conn)), error = function(e) FAIL(e))
            PASS()
