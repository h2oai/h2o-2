            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_train_7113e248_c3f2_4b1d_b259_0c272fcb0ce6 <- function(conn) {
                Log.info("A munge-task R unit test on data <train> testing the functional unit <[> ")
                Log.info("Uploading train")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x500/R/train.csv", "train.hex")
                Log.info("Performing a column slice of train using these columns: c(\"x\")")
                slicedHex <- hex[,c("x")]
                    Log.info("Performing a 1-by-1 column slice of train using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of train using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data train", sliceTest_train_7113e248_c3f2_4b1d_b259_0c272fcb0ce6(conn)), error = function(e) FAIL(e))
            PASS()
