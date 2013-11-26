            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_test_numeric_extra_5b1083b7_5916_4a12_b414_abf1b58c6ea0 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris_test_numeric_extra> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading iris_test_numeric_extra")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/classifier/iris_test_numeric_extra.csv", "iris_test_numeric_extra.hex")
            Log.info("Performing compound task !( ( ( hex[,c(\"species\")] != 3.25963092515 )) ) on dataset <iris_test_numeric_extra>")
                     filterHex <- hex[!( ( ( hex[,c("species")] != 3.25963092515 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(\"petal_wid\")] != 1.74415042138 )) ) on dataset iris_test_numeric_extra, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c("petal_wid")] != 1.74415042138 )) ), c("petal_wid","sepal_wid")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c("petal_wid")] != 1.74415042138 )) ), c("petal_len","sepal_len","species")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris_test_numeric_extra", complexFilterTest_iris_test_numeric_extra_5b1083b7_5916_4a12_b414_abf1b58c6ea0(conn)), error = function(e) FAIL(e))
            PASS()
