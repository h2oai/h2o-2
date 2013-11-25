            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_test_numeric_extra_07aee9cd_39cf_49cb_895c_f795c73c8355 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris_test_numeric_extra> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading iris_test_numeric_extra")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/classifier/iris_test_numeric_extra.csv", "iris_test_numeric_extra.hex")
            Log.info("Performing compound task ( hex[,c(\"sepal_len\")] != 5.40968097651 ) | ( hex[,c(\"petal_wid\")] < 0.613893299137 ) & ( ( hex[,c(\"petal_len\")] < 3.08970178329 ))  on dataset <iris_test_numeric_extra>")
                     filterHex <- hex[( hex[,c("sepal_len")] != 5.40968097651 ) | ( hex[,c("petal_wid")] < 0.613893299137 ) & ( ( hex[,c("petal_len")] < 3.08970178329 )) ,]
            Log.info("Performing compound task ( hex[,c(\"sepal_wid\")] != 3.68915030439 ) | ( hex[,c(\"species\")] < 1.33790958516 ) & ( ( hex[,c(\"sepal_len\")] < 4.3498708073 ))  on dataset iris_test_numeric_extra, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("sepal_wid")] != 3.68915030439 ) | ( hex[,c("species")] < 1.33790958516 ) & ( ( hex[,c("sepal_len")] < 4.3498708073 )) , c("petal_wid","sepal_wid","petal_len","sepal_len","species")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("sepal_wid")] != 3.68915030439 ) | ( hex[,c("species")] < 1.33790958516 ) & ( ( hex[,c("sepal_len")] < 4.3498708073 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris_test_numeric_extra", complexFilterTest_iris_test_numeric_extra_07aee9cd_39cf_49cb_895c_f795c73c8355(conn)), error = function(e) FAIL(e))
            PASS()
