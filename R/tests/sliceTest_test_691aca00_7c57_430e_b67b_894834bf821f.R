            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_test_691aca00_7c57_430e_b67b_894834bf821f <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the functional unit <[> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x10/weka/test.csv.arff", "test.hex")
                    Log.info("Performing a row slice of test using these rows: c(1,3,2,5,4,7,6,8)")
                slicedHex <- hex[c(1,3,2,5,4,7,6,8),]
                    Log.info("Performing a row & column slice of test using these rows & columns: c(1) & c(1)")
                slicedHex <- hex[c(1),c(1)]
                    Log.info("Performing a 1-by-1 column slice of test using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of test using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data test", sliceTest_test_691aca00_7c57_430e_b67b_894834bf821f(conn)), error = function(e) FAIL(e))
            PASS()
