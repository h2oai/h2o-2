            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_test_78053a03_a8d0_4238_bc05_4d5d871cb2c6 <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the functional unit <[> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x100/R/test.csv", "test.hex")
                Log.info("Performing a column slice of test using these columns: c(1)")
                slicedHex <- hex[,c(1)]
                    Log.info("Performing a row slice of test using these rows: c(56,54,42,48,43,60,61,62,63,64,49,66,67,68,69,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,65,75,38,73,72,71,70,59,58,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,55,74,32,57,50)")
                slicedHex <- hex[c(56,54,42,48,43,60,61,62,63,64,49,66,67,68,69,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,65,75,38,73,72,71,70,59,58,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,55,74,32,57,50),]
                    Log.info("Performing a row & column slice of test using these rows & columns: c(1) & c(24,39,89,17,52,133,74,124,127,82,103,69,104,96)")
                slicedHex <- hex[c(24,39,89,17,52,133,74,124,127,82,103,69,104,96),c(1)]
                    Log.info("Performing a 1-by-1 column slice of test using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of test using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data test", sliceTest_test_78053a03_a8d0_4238_bc05_4d5d871cb2c6(conn)), error = function(e) FAIL(e))
            PASS()
