            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_iris_test_missing_fc0df152_20cb_4a21_ac71_4424a241dff0 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris_test_missing> testing the functional unit <[> ")
                Log.info("Uploading iris_test_missing")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/classifier/iris_test_missing.csv", "iris_test_missing.hex")
                    Log.info("Performing a 1-by-1 column slice of iris_test_missing using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of iris_test_missing using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data iris_test_missing", sliceTest_iris_test_missing_fc0df152_20cb_4a21_ac71_4424a241dff0(conn)), error = function(e) FAIL(e))
            PASS()
