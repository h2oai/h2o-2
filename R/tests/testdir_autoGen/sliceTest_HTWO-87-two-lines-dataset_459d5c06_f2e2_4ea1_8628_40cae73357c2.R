            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_HTWO-87-two-lines-dataset_459d5c06_f2e2_4ea1_8628_40cae73357c2 <- function(conn) {
                Log.info("A munge-task R unit test on data <HTWO-87-two-lines-dataset> testing the functional unit <[> ")
                Log.info("Uploading HTWO-87-two-lines-dataset")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/HTWO-87-two-lines-dataset.csv", "HTWO-87-two-lines-dataset.hex")
                    Log.info("Performing a row slice of HTWO-87-two-lines-dataset using these rows: c(1)")
                slicedHex <- hex[c(1),]
                    Log.info("Performing a row & column slice of HTWO-87-two-lines-dataset using these rows & columns: c(1,3,8) & c(1)")
                slicedHex <- hex[c(1),c(1,3,8)]
                    Log.info("Performing a 1-by-1 column slice of HTWO-87-two-lines-dataset using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of HTWO-87-two-lines-dataset using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data HTWO-87-two-lines-dataset", sliceTest_HTWO-87-two-lines-dataset_459d5c06_f2e2_4ea1_8628_40cae73357c2(conn)), error = function(e) FAIL(e))
            PASS()
