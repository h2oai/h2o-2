            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_prostate_5_7126ceae_e4e8_4e5e_96d1_319df152f05a <- function(conn) {
                Log.info("A munge-task R unit test on data <prostate_5> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading prostate_5")
                hex <- h2o.uploadFile(conn, "../../../smalldata/parse_folder_test/prostate_5.csv", "prostate_5.hex")
            Log.info("Performing compound task ( hex[,c(4)] != 1.54065488322 ) | ( hex[,c(2)] == 68.6857719705 ) & ( hex[,c(6)] !| 55.4060978402 ) ! ( hex[,c(8)] > 7.20719561266 ) on dataset <prostate_5>")
                     filterHex <- hex[( hex[,c(4)] != 1.54065488322 ) | ( hex[,c(2)] == 68.6857719705 ) & ( hex[,c(6)] !| 55.4060978402 ) ! ( hex[,c(8)] > 7.20719561266 ),]
            Log.info("Performing compound task ( hex[,c(5)] != 1.07231520742 ) | ( hex[,c(8)] == 5.85147997672 ) & ( hex[,c(8)] !| 6.2737239243 ) ! ( hex[,c(8)] > 7.64222552402 ) on dataset prostate_5, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(5)] != 1.07231520742 ) | ( hex[,c(8)] == 5.85147997672 ) & ( hex[,c(8)] !| 6.2737239243 ) ! ( hex[,c(8)] > 7.64222552402 ), c(1,5,4,7,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(5)] != 1.07231520742 ) | ( hex[,c(8)] == 5.85147997672 ) & ( hex[,c(8)] !| 6.2737239243 ) ! ( hex[,c(8)] > 7.64222552402 ), c(3,2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data prostate_5", complexFilterTest_prostate_5_7126ceae_e4e8_4e5e_96d1_319df152f05a(conn)), error = function(e) FAIL(e))
            PASS()
