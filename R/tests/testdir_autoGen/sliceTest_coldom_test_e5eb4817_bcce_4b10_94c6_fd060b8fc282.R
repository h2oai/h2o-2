            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_coldom_test_e5eb4817_bcce_4b10_94c6_fd060b8fc282 <- function(conn) {
                Log.info("A munge-task R unit test on data <coldom_test> testing the functional unit <[> ")
                Log.info("Uploading coldom_test")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/classifier/coldom_test.csv", "coldom_test.hex")
                Log.info("Performing a column slice of coldom_test using these columns: c(1)")
                slicedHex <- hex[,c(1)]
                    Log.info("Performing a row slice of coldom_test using these rows: c(1)")
                slicedHex <- hex[c(1),]
                    Log.info("Performing a 1-by-1 column slice of coldom_test using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of coldom_test using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data coldom_test", sliceTest_coldom_test_e5eb4817_bcce_4b10_94c6_fd060b8fc282(conn)), error = function(e) FAIL(e))
            PASS()
