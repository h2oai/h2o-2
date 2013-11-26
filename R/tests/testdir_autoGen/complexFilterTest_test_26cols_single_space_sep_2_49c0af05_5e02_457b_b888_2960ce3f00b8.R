            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_26cols_single_space_sep_2_49c0af05_5e02_457b_b888_2960ce3f00b8 <- function(conn) {
                Log.info("A munge-task R unit test on data <test_26cols_single_space_sep_2> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading test_26cols_single_space_sep_2")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/test_26cols_single_space_sep_2.csv", "test_26cols_single_space_sep_2.hex")
            Log.info("Performing compound task ( hex[,c(\"R\")] != 18.0 ) | ( hex[,c(\"A\")] < 1.0 ) & ( ( hex[,c(\"A\")] < 1.0 ))  on dataset <test_26cols_single_space_sep_2>")
                     filterHex <- hex[( hex[,c("R")] != 18.0 ) | ( hex[,c("A")] < 1.0 ) & ( ( hex[,c("A")] < 1.0 )) ,]
            Log.info("Performing compound task ( hex[,c(\"U\")] != 21.0 ) | ( hex[,c(\"J\")] < 10.0 ) & ( ( hex[,c(\"J\")] < 10.0 ))  on dataset test_26cols_single_space_sep_2, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("U")] != 21.0 ) | ( hex[,c("J")] < 10.0 ) & ( ( hex[,c("J")] < 10.0 )) , c("M","P","J","E","H","C","V","A","Z","U","N","R","L")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("U")] != 21.0 ) | ( hex[,c("J")] < 10.0 ) & ( ( hex[,c("J")] < 10.0 )) , c("F","G","Q","K","D","I","B","W","T","O","X","Y","S")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test_26cols_single_space_sep_2", complexFilterTest_test_26cols_single_space_sep_2_49c0af05_5e02_457b_b888_2960ce3f00b8(conn)), error = function(e) FAIL(e))
            PASS()
