            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_pros_954f25a4_d557_4118_98ad_fa35320a866a <- function(conn) {
                Log.info("A munge-task R unit test on data <pros> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading pros")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/pros.xls", "pros.hex")
            Log.info("Performing compound task ( hex[,c(\"DPROS\")] != 2.0810415601 ) | ( hex[,c(\"CAPSULE\")] == 0.396483907428 ) & ( hex[,c(\"DPROS\")] !| 3.38497916275 ) ! ( hex[,c(\"DPROS\")] > 3.39534283497 ) on dataset <pros>")
                     filterHex <- hex[( hex[,c("DPROS")] != 2.0810415601 ) | ( hex[,c("CAPSULE")] == 0.396483907428 ) & ( hex[,c("DPROS")] !| 3.38497916275 ) ! ( hex[,c("DPROS")] > 3.39534283497 ),]
            Log.info("Performing compound task ( hex[,c(\"DCAPS\")] != 1.04419172875 ) | ( hex[,c(\"GLEASON\")] == 8.87977878542 ) & ( hex[,c(\"DCAPS\")] !| 1.33281639817 ) ! ( hex[,c(\"ID\")] > 106.251863693 ) on dataset pros, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("DCAPS")] != 1.04419172875 ) | ( hex[,c("GLEASON")] == 8.87977878542 ) & ( hex[,c("DCAPS")] !| 1.33281639817 ) ! ( hex[,c("ID")] > 106.251863693 ), c("GLEASON","RACE","PSA","DCAPS","VOL","CAPSULE","DPROS","ID","AGE")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("DCAPS")] != 1.04419172875 ) | ( hex[,c("GLEASON")] == 8.87977878542 ) & ( hex[,c("DCAPS")] !| 1.33281639817 ) ! ( hex[,c("ID")] > 106.251863693 ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data pros", complexFilterTest_pros_954f25a4_d557_4118_98ad_fa35320a866a(conn)), error = function(e) FAIL(e))
            PASS()
