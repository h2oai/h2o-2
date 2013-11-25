            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_poisson_tst1_803598d6_4de8_4c8f_8aed_3b94824e9982 <- function(conn) {
                Log.info("A munge-task R unit test on data <poisson_tst1> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading poisson_tst1")
                hex <- h2o.uploadFile(conn, "../../../smalldata/glm_test/poisson_tst1.csv", "poisson_tst1.hex")
            Log.info("Performing compound task ( hex[,c(\"math\")] != 38.3506700296 ) | ( hex[,c(\"num_awards\")] < 4.69959703842 ) & ( ( hex[,c(\"math\")] < 74.1367450852 ))  on dataset <poisson_tst1>")
                     filterHex <- hex[( hex[,c("math")] != 38.3506700296 ) | ( hex[,c("num_awards")] < 4.69959703842 ) & ( ( hex[,c("math")] < 74.1367450852 )) ,]
            Log.info("Performing compound task ( hex[,c(\"id\")] != 141.074838348 ) | ( hex[,c(\"math\")] < 66.9944317473 ) & ( ( hex[,c(\"id\")] < 122.520633779 ))  on dataset poisson_tst1, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("id")] != 141.074838348 ) | ( hex[,c("math")] < 66.9944317473 ) & ( ( hex[,c("id")] < 122.520633779 )) , c("math","id","num_awards")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("id")] != 141.074838348 ) | ( hex[,c("math")] < 66.9944317473 ) & ( ( hex[,c("id")] < 122.520633779 )) , c("prog")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data poisson_tst1", complexFilterTest_poisson_tst1_803598d6_4de8_4c8f_8aed_3b94824e9982(conn)), error = function(e) FAIL(e))
            PASS()
