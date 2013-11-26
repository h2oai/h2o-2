            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_prostate_long_0f42cf01_2401_4451_843a_e37960ef29e6 <- function(conn) {
                Log.info("A munge-task R unit test on data <prostate_long> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading prostate_long")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/prostate_long.csv.gz", "prostate_long.hex")
            Log.info("Performing compound task ( hex[,c(\"DPROS\")] != 1.28202126296 ) | ( hex[,c(\"AGE\")] == 54.7482720013 ) & ( hex[,c(\"DCAPS\")] !| 1.55989658486 ) ! ( hex[,c(\"ID\")] > 357.914886669 ) on dataset <prostate_long>")
                     filterHex <- hex[( hex[,c("DPROS")] != 1.28202126296 ) | ( hex[,c("AGE")] == 54.7482720013 ) & ( hex[,c("DCAPS")] !| 1.55989658486 ) ! ( hex[,c("ID")] > 357.914886669 ),]
            Log.info("Performing compound task ( hex[,c(\"DCAPS\")] != 1.1872418779 ) | ( hex[,c(\"AGE\")] == 53.2849865055 ) & ( hex[,c(\"GLEASON\")] !| 7.37482246389 ) ! ( hex[,c(\"DCAPS\")] > 1.60384483475 ) on dataset prostate_long, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("DCAPS")] != 1.1872418779 ) | ( hex[,c("AGE")] == 53.2849865055 ) & ( hex[,c("GLEASON")] !| 7.37482246389 ) ! ( hex[,c("DCAPS")] > 1.60384483475 ), c("ID","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","GLEASON","AGE")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("DCAPS")] != 1.1872418779 ) | ( hex[,c("AGE")] == 53.2849865055 ) & ( hex[,c("GLEASON")] !| 7.37482246389 ) ! ( hex[,c("DCAPS")] > 1.60384483475 ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data prostate_long", complexFilterTest_prostate_long_0f42cf01_2401_4451_843a_e37960ef29e6(conn)), error = function(e) FAIL(e))
            PASS()
