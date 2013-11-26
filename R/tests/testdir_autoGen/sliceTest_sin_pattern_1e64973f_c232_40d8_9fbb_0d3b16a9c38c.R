            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_sin_pattern_1e64973f_c232_40d8_9fbb_0d3b16a9c38c <- function(conn) {
                Log.info("A munge-task R unit test on data <sin_pattern> testing the functional unit <[> ")
                Log.info("Uploading sin_pattern")
                hex <- h2o.uploadFile(conn, "../../../smalldata/neural/sin_pattern.data", "sin_pattern.hex")
                Log.info("Performing a column slice of sin_pattern using these columns: c(1)")
                slicedHex <- hex[,c(1)]
                    Log.info("Performing a 1-by-1 column slice of sin_pattern using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of sin_pattern using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data sin_pattern", sliceTest_sin_pattern_1e64973f_c232_40d8_9fbb_0d3b16a9c38c(conn)), error = function(e) FAIL(e))
            PASS()
