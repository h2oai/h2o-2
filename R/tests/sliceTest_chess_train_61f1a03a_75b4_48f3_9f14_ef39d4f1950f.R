            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_chess_train_61f1a03a_75b4_48f3_9f14_ef39d4f1950f <- function(conn) {
                Log.info("A munge-task R unit test on data <chess_train> testing the functional unit <[> ")
                Log.info("Uploading chess_train")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/classifier/chess_train.csv", "chess_train.hex")
                    Log.info("Performing a row slice of chess_train using these rows: c(11,10,13,12,15,14,17,16,18,1,3,2,5,4,7,6,9,8)")
                slicedHex <- hex[c(11,10,13,12,15,14,17,16,18,1,3,2,5,4,7,6,9,8),]
                    Log.info("Performing a row & column slice of chess_train using these rows & columns: c(\"x\") & c(3,4,6)")
                slicedHex <- hex[c(3,4,6),c("x")]
                    Log.info("Performing a 1-by-1 column slice of chess_train using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of chess_train using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data chess_train", sliceTest_chess_train_61f1a03a_75b4_48f3_9f14_ef39d4f1950f(conn)), error = function(e) FAIL(e))
            PASS()
