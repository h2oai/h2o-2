            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_prostate_6_29a10f0c_c7b1_4131_93b7_8431ec825590 <- function(conn) {
                Log.info("A munge-task R unit test on data <prostate_6> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading prostate_6")
                hex <- h2o.uploadFile(conn, "../../smalldata/parse_folder_test/prostate_6.csv", "prostate_6.hex")
            Log.info("Performing compound task ( hex[,c(1)] != 229.389824333 ) | ( hex[,c(5)] == 1.74352929611 ) & ( hex[,c(2)] !| 62.358682811 ) ! ( hex[,c(4)] > 2.53888172977 ) on dataset <prostate_6>")
                     filterHex <- hex[( hex[,c(1)] != 229.389824333 ) | ( hex[,c(5)] == 1.74352929611 ) & ( hex[,c(2)] !| 62.358682811 ) ! ( hex[,c(4)] > 2.53888172977 ),]
            Log.info("Performing compound task ( hex[,c(7)] != 40.4646975027 ) | ( hex[,c(4)] == 2.0133526871 ) & ( hex[,c(2)] !| 50.8168079701 ) ! ( hex[,c(2)] > 57.1398307351 ) on dataset prostate_6, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(7)] != 40.4646975027 ) | ( hex[,c(4)] == 2.0133526871 ) & ( hex[,c(2)] !| 50.8168079701 ) ! ( hex[,c(2)] > 57.1398307351 ), c(1,2,5,4,7,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(7)] != 40.4646975027 ) | ( hex[,c(4)] == 2.0133526871 ) & ( hex[,c(2)] !| 50.8168079701 ) ! ( hex[,c(2)] > 57.1398307351 ), c(3)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data prostate_6", complexFilterTest_prostate_6_29a10f0c_c7b1_4131_93b7_8431ec825590(conn)), error = function(e) FAIL(e))
            PASS()
