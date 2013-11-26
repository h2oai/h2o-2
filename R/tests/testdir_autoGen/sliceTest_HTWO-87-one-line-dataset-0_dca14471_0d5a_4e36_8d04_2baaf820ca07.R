            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_HTWO-87-one-line-dataset-0_dca14471_0d5a_4e36_8d04_2baaf820ca07 <- function(conn) {
                Log.info("A munge-task R unit test on data <HTWO-87-one-line-dataset-0> testing the functional unit <[> ")
                Log.info("Uploading HTWO-87-one-line-dataset-0")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/HTWO-87-one-line-dataset-0.csv", "HTWO-87-one-line-dataset-0.hex")
                    Log.info("Performing a 1-by-1 column slice of HTWO-87-one-line-dataset-0 using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of HTWO-87-one-line-dataset-0 using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data HTWO-87-one-line-dataset-0", sliceTest_HTWO-87-one-line-dataset-0_dca14471_0d5a_4e36_8d04_2baaf820ca07(conn)), error = function(e) FAIL(e))
            PASS()
