            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_prostate_1_3831951b_a33b_4756_8694_38ea31ad2b2e <- function(conn) {
                Log.info("A munge-task R unit test on data <prostate_1> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading prostate_1")
                hex <- h2o.uploadFile(conn, "../../smalldata/parse_folder_test/prostate_1.csv", "prostate_1.hex")
            Log.info("Performing compound task ( hex[,c(2)] != 55.4505168177 ) | ( hex[,c(8)] < 7.97353644319 ) & ( ( hex[,c(5)] < 1.80726315914 ))  on dataset <prostate_1>")
                     filterHex <- hex[( hex[,c(2)] != 55.4505168177 ) | ( hex[,c(8)] < 7.97353644319 ) & ( ( hex[,c(5)] < 1.80726315914 )) ,]
            Log.info("Performing compound task ( hex[,c(4)] != 2.46584559732 ) | ( hex[,c(8)] < 6.62348291187 ) & ( ( hex[,c(5)] < 1.30149091087 ))  on dataset prostate_1, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(4)] != 2.46584559732 ) | ( hex[,c(8)] < 6.62348291187 ) & ( ( hex[,c(5)] < 1.30149091087 )) , c(1,2,5,4,7,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(4)] != 2.46584559732 ) | ( hex[,c(8)] < 6.62348291187 ) & ( ( hex[,c(5)] < 1.30149091087 )) , c(3)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data prostate_1", complexFilterTest_prostate_1_3831951b_a33b_4756_8694_38ea31ad2b2e(conn)), error = function(e) FAIL(e))
            PASS()
