            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_iris_ea6ee90e_069e_4817_aac6_0aebec738912 <- function(conn) {
                Log.info("A munge-task R unit test on data <iris> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading iris")
                hex <- h2o.uploadFile(conn, "../../smalldata/iris/iris.csv.gz", "iris.hex")
            Log.info("Performing compound task !( ( ( hex[,c(3)] != 0.874937361729 )) ) on dataset <iris>")
                     filterHex <- hex[!( ( ( hex[,c(3)] != 0.874937361729 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(3)] != 0.777307927895 )) ) on dataset iris, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(3)] != 0.777307927895 )) ), c(1,3,2)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(3)] != 0.777307927895 )) ), c(4)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data iris", complexFilterTest_iris_ea6ee90e_069e_4817_aac6_0aebec738912(conn)), error = function(e) FAIL(e))
            PASS()
