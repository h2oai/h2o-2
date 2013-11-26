            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_905c80b1_146b_43c3_8fd3_f43938241bad <- function(conn) {
                Log.info("A munge-task R unit test on data <iris> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading iris")
                hex <- h2o.uploadFile(conn, "../../../smalldata/iris/iris.csv.gz", "iris.hex")
            Log.info("Performing compound task !( ( hex[,c(3)] <= 0.495548637111 ) | ( hex[,c(1)] == 2.69600926676 ) & ( hex[,c(1)] <= 2.208941896 ) & ( hex[,c(1)] <= 5.60361285268 ) & ( ( hex[,c(2)] <= 3.52892557914 )) ) on dataset <iris>")
                     filterHex <- hex[!( ( hex[,c(3)] <= 0.495548637111 ) | ( hex[,c(1)] == 2.69600926676 ) & ( hex[,c(1)] <= 2.208941896 ) & ( hex[,c(1)] <= 5.60361285268 ) & ( ( hex[,c(2)] <= 3.52892557914 )) ),]
            Log.info("Performing compound task !( ( hex[,c(3)] <= 1.84622371075 ) | ( hex[,c(1)] == 3.34688447608 ) & ( hex[,c(2)] <= 5.988281895 ) & ( hex[,c(1)] <= 2.84119631713 ) & ( ( hex[,c(1)] <= 7.68713965438 )) ) on dataset iris, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c(3)] <= 1.84622371075 ) | ( hex[,c(1)] == 3.34688447608 ) & ( hex[,c(2)] <= 5.988281895 ) & ( hex[,c(1)] <= 2.84119631713 ) & ( ( hex[,c(1)] <= 7.68713965438 )) ), c(1,3,2)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c(3)] <= 1.84622371075 ) | ( hex[,c(1)] == 3.34688447608 ) & ( hex[,c(2)] <= 5.988281895 ) & ( hex[,c(1)] <= 2.84119631713 ) & ( ( hex[,c(1)] <= 7.68713965438 )) ), c(4)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris", complexFilterTest_iris_905c80b1_146b_43c3_8fd3_f43938241bad(conn)), error = function(e) FAIL(e))
            PASS()
