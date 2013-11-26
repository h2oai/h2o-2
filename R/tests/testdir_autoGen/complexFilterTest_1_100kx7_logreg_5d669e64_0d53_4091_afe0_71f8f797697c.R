            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_1_100kx7_logreg_5d669e64_0d53_4091_afe0_71f8f797697c <- function(conn) {
                Log.info("A munge-task R unit test on data <1_100kx7_logreg> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading 1_100kx7_logreg")
                hex <- h2o.uploadFile(conn, "../../../smalldata/1_100kx7_logreg.data.gz", "1_100kx7_logreg.hex")
            Log.info("Performing compound task !( ( hex[,c(1)] <= 4.19218841565 ) | ( hex[,c(1)] == 10.5001754137 ) & ( hex[,c(4)] <= 9.90279807272 ) & ( hex[,c(5)] <= -0.0519964799271 ) & ( ( hex[,c(6)] <= 47.6711913854 )) ) on dataset <1_100kx7_logreg>")
                     filterHex <- hex[!( ( hex[,c(1)] <= 4.19218841565 ) | ( hex[,c(1)] == 10.5001754137 ) & ( hex[,c(4)] <= 9.90279807272 ) & ( hex[,c(5)] <= -0.0519964799271 ) & ( ( hex[,c(6)] <= 47.6711913854 )) ),]
            Log.info("Performing compound task !( ( hex[,c(4)] <= 14.8678524116 ) | ( hex[,c(1)] == 8.58117176927 ) & ( hex[,c(6)] <= 349.256492471 ) & ( hex[,c(4)] <= 18.5258253533 ) & ( ( hex[,c(4)] <= 6.90530775855 )) ) on dataset 1_100kx7_logreg, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c(4)] <= 14.8678524116 ) | ( hex[,c(1)] == 8.58117176927 ) & ( hex[,c(6)] <= 349.256492471 ) & ( hex[,c(4)] <= 18.5258253533 ) & ( ( hex[,c(4)] <= 6.90530775855 )) ), c(1,3,2,5,4,7,6)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c(4)] <= 14.8678524116 ) | ( hex[,c(1)] == 8.58117176927 ) & ( hex[,c(6)] <= 349.256492471 ) & ( hex[,c(4)] <= 18.5258253533 ) & ( ( hex[,c(4)] <= 6.90530775855 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data 1_100kx7_logreg", complexFilterTest_1_100kx7_logreg_5d669e64_0d53_4091_afe0_71f8f797697c(conn)), error = function(e) FAIL(e))
            PASS()
