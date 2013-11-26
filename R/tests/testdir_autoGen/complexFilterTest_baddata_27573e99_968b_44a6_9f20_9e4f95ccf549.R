            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_baddata_27573e99_968b_44a6_9f20_9e4f95ccf549 <- function(conn) {
                Log.info("A munge-task R unit test on data <baddata> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading baddata")
                hex <- h2o.uploadFile(conn, "../../../smalldata/baddata.data", "baddata.hex")
            Log.info("Performing compound task !( ( ( hex[,c(1)] != 10.5407299971 )) ) on dataset <baddata>")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 10.5407299971 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(1)] != 4.48314687698 )) ) on dataset baddata, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 4.48314687698 )) ), c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c(1)] != 4.48314687698 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data baddata", complexFilterTest_baddata_27573e99_968b_44a6_9f20_9e4f95ccf549(conn)), error = function(e) FAIL(e))
            PASS()
