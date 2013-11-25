            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_chdage_cleaned_345ecbfd_f3f6_4d58_933f_50700e7747ba <- function(conn) {
                Log.info("A munge-task R unit test on data <chdage_cleaned> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading chdage_cleaned")
                hex <- h2o.uploadFile(conn, "../../smalldata/logreg/umass_statdata/chdage_cleaned.dat", "chdage_cleaned.hex")
            Log.info("Performing compound task !( ( hex[,c(1)] <= 22.9613840654 ) | ( hex[,c(1)] == 86.2085583973 ) & ( hex[,c(1)] <= 47.8805512685 ) & ( hex[,c(2)] <= 0.435276783181 ) & ( ( hex[,c(1)] <= 5.70454593138 )) ) on dataset <chdage_cleaned>")
                     filterHex <- hex[!( ( hex[,c(1)] <= 22.9613840654 ) | ( hex[,c(1)] == 86.2085583973 ) & ( hex[,c(1)] <= 47.8805512685 ) & ( hex[,c(2)] <= 0.435276783181 ) & ( ( hex[,c(1)] <= 5.70454593138 )) ),]
            Log.info("Performing compound task !( ( hex[,c(1)] <= 22.2959488932 ) | ( hex[,c(1)] == 58.7511572311 ) & ( hex[,c(1)] <= 10.7986949624 ) & ( hex[,c(2)] <= 0.569495396766 ) & ( ( hex[,c(1)] <= 1.38984410836 )) ) on dataset chdage_cleaned, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c(1)] <= 22.2959488932 ) | ( hex[,c(1)] == 58.7511572311 ) & ( hex[,c(1)] <= 10.7986949624 ) & ( hex[,c(2)] <= 0.569495396766 ) & ( ( hex[,c(1)] <= 1.38984410836 )) ), c(1,2)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c(1)] <= 22.2959488932 ) | ( hex[,c(1)] == 58.7511572311 ) & ( hex[,c(1)] <= 10.7986949624 ) & ( hex[,c(2)] <= 0.569495396766 ) & ( ( hex[,c(1)] <= 1.38984410836 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data chdage_cleaned", complexFilterTest_chdage_cleaned_345ecbfd_f3f6_4d58_933f_50700e7747ba(conn)), error = function(e) FAIL(e))
            PASS()
