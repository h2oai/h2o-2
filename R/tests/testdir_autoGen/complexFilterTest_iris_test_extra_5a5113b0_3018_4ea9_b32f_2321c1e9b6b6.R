            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_test_extra_5a5113b0_3018_4ea9_b32f_2321c1e9b6b6 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris_test_extra> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading iris_test_extra")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/classifier/iris_test_extra.csv", "iris_test_extra.hex")
            Log.info("Performing compound task ( hex[,c(\"petal_len\")] != 4.15059341256 ) | ( hex[,c(\"petal_len\")] == 4.31148610939 ) & ( hex[,c(\"petal_len\")] !| 4.87775069145 ) ! ( hex[,c(\"petal_len\")] > 1.99767944092 ) on dataset <iris_test_extra>")
                     filterHex <- hex[( hex[,c("petal_len")] != 4.15059341256 ) | ( hex[,c("petal_len")] == 4.31148610939 ) & ( hex[,c("petal_len")] !| 4.87775069145 ) ! ( hex[,c("petal_len")] > 1.99767944092 ),]
            Log.info("Performing compound task ( hex[,c(\"sepal_len\")] != 7.4405832648 ) | ( hex[,c(\"petal_len\")] == 6.64810469106 ) & ( hex[,c(\"sepal_len\")] !| 6.21469097228 ) ! ( hex[,c(\"petal_wid\")] > 0.914709791283 ) on dataset iris_test_extra, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("sepal_len")] != 7.4405832648 ) | ( hex[,c("petal_len")] == 6.64810469106 ) & ( hex[,c("sepal_len")] !| 6.21469097228 ) ! ( hex[,c("petal_wid")] > 0.914709791283 ), c("petal_wid","sepal_wid","petal_len","sepal_len")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("sepal_len")] != 7.4405832648 ) | ( hex[,c("petal_len")] == 6.64810469106 ) & ( hex[,c("sepal_len")] !| 6.21469097228 ) ! ( hex[,c("petal_wid")] > 0.914709791283 ), c("species")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris_test_extra", complexFilterTest_iris_test_extra_5a5113b0_3018_4ea9_b32f_2321c1e9b6b6(conn)), error = function(e) FAIL(e))
            PASS()
