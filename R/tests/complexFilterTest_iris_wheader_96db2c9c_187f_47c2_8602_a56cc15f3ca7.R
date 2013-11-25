            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_wheader_96db2c9c_187f_47c2_8602_a56cc15f3ca7 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris_wheader> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading iris_wheader")
                hex <- h2o.uploadFile(conn, "../../smalldata/iris/iris_wheader.csv.zip", "iris_wheader.hex")
            Log.info("Performing compound task !( ( hex[,c(\"sepal_wid\")] <= 3.39921826131 ) | ( hex[,c(\"petal_wid\")] == 1.31023039871 )) on dataset <iris_wheader>")
                     filterHex <- hex[!( ( hex[,c("sepal_wid")] <= 3.39921826131 ) | ( hex[,c("petal_wid")] == 1.31023039871 )),]
            Log.info("Performing compound task !( ( hex[,c(\"petal_len\")] <= 3.0072079239 ) | ( hex[,c(\"sepal_wid\")] == 2.3168974963 )) on dataset iris_wheader, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("petal_len")] <= 3.0072079239 ) | ( hex[,c("sepal_wid")] == 2.3168974963 )), c("sepal_wid","petal_len")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("petal_len")] <= 3.0072079239 ) | ( hex[,c("sepal_wid")] == 2.3168974963 )), c("petal_wid","class","sepal_len")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris_wheader", complexFilterTest_iris_wheader_96db2c9c_187f_47c2_8602_a56cc15f3ca7(conn)), error = function(e) FAIL(e))
            PASS()
