            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_baddata_2b1dc302_7437_46b8_853a_e935ee685176 <- function(conn) {
                Log.info("A munge-task R unit test on data <baddata> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading baddata")
                hex <- h2o.uploadFile(conn, "../../smalldata/baddata.data", "baddata.hex")
            Log.info("Performing compound task ( hex[,c(1)] != 20.5404642092 ) | ( hex[,c(1)] < 0.0446886456111 ) & ( ( hex[,c(1)] < 4.50681616077 ))  on dataset <baddata>")
                     filterHex <- hex[( hex[,c(1)] != 20.5404642092 ) | ( hex[,c(1)] < 0.0446886456111 ) & ( ( hex[,c(1)] < 4.50681616077 )) ,]
            Log.info("Performing compound task ( hex[,c(1)] != 13.7168229624 ) | ( hex[,c(1)] < 0.609632565351 ) & ( ( hex[,c(1)] < 7.71408242318 ))  on dataset baddata, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 13.7168229624 ) | ( hex[,c(1)] < 0.609632565351 ) & ( ( hex[,c(1)] < 7.71408242318 )) , c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 13.7168229624 ) | ( hex[,c(1)] < 0.609632565351 ) & ( ( hex[,c(1)] < 7.71408242318 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data baddata", complexFilterTest_baddata_2b1dc302_7437_46b8_853a_e935ee685176(conn)), error = function(e) FAIL(e))
            PASS()
