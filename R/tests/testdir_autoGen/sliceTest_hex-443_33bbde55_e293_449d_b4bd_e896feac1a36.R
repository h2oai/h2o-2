            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_hex-443_33bbde55_e293_449d_b4bd_e896feac1a36 <- function(conn) {
                Log.info("A munge-task R unit test on data <hex-443> testing the functional unit <[> ")
                Log.info("Uploading hex-443")
                hex <- h2o.uploadFile(conn, "../../../smalldata/hex-443.parsetmp_1_0_0_0.data", "hex-443.hex")
                Log.info("Performing a column slice of hex-443 using these columns: c(\"MiddleInitials\",\"FirstName\")")
                slicedHex <- hex[,c("MiddleInitials","FirstName")]
                    Log.info("Performing a 1-by-1 column slice of hex-443 using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of hex-443 using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data hex-443", sliceTest_hex-443_33bbde55_e293_449d_b4bd_e896feac1a36(conn)), error = function(e) FAIL(e))
            PASS()
