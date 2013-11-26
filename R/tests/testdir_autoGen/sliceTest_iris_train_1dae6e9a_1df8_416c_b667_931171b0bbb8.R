            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_iris_train_1dae6e9a_1df8_416c_b667_931171b0bbb8 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris_train> testing the functional unit <[> ")
                Log.info("Uploading iris_train")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/classifier/iris_train.csv", "iris_train.hex")
                Log.info("Performing a column slice of iris_train using these columns: c(\"sepal_wid\",\"petal_len\",\"sepal_len\")")
                slicedHex <- hex[,c("sepal_wid","petal_len","sepal_len")]
                    Log.info("Performing a row slice of iris_train using these rows: c(56,77,54,42,48,43,60,61,62,63,64,49,66,67,68,69,80,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,78,51,39,65,76,75,38,73,72,71,70,59,79,58,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,55,74,32,57,50)")
                slicedHex <- hex[c(56,77,54,42,48,43,60,61,62,63,64,49,66,67,68,69,80,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,78,51,39,65,76,75,38,73,72,71,70,59,79,58,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,55,74,32,57,50),]
                    Log.info("Performing a 1-by-1 column slice of iris_train using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of iris_train using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data iris_train", sliceTest_iris_train_1dae6e9a_1df8_416c_b667_931171b0bbb8(conn)), error = function(e) FAIL(e))
            PASS()
