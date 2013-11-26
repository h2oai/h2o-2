            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_syn_sphere2_14ad28ee_07d7_48b8_9182_81e79b74e177 <- function(conn) {
                Log.info("A munge-task R unit test on data <syn_sphere2> testing the functional unit <>=> ")
                Log.info("Uploading syn_sphere2")
                hex <- h2o.uploadFile(conn, "../../../smalldata/syn_sphere2.csv", "syn_sphere2.hex")
                Log.info("Filtering out rows by >= from dataset syn_sphere2 and column \"1\" using value 14.4550971892")
                     filterHex <- hex[hex[,c(1)] >= 14.4550971892,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 14.4550971892,]
                Log.info("Filtering out rows by >= from dataset syn_sphere2 and column \"2\" using value 47.3822016289")
                     filterHex <- hex[hex[,c(2)] >= 47.3822016289,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"2" >= 47.3822016289,]
                Log.info("Filtering out rows by >= from dataset syn_sphere2 and column \"0\" using value -18.4705437538")
                     filterHex <- hex[hex[,c(1)] >= -18.4705437538,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= -18.4705437538,]
                    Log.info("Filtering out rows by >= from dataset syn_sphere2 and column \"2\" using value 43.0569370137, and also subsetting columns.")
                     filterHex <- hex[hex[,c(2)] >= 43.0569370137, c(2)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(2)] >= 43.0569370137, c(1,2)]
                    Log.info("Filtering out rows by >= from dataset syn_sphere2 and column \"0\" using value 6.30519916446, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 6.30519916446, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 6.30519916446, c(1,2)]
                    Log.info("Filtering out rows by >= from dataset syn_sphere2 and column \"0\" using value -10.984511801, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= -10.984511801, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= -10.984511801, c(1,2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data syn_sphere2", simpleFilterTest_syn_sphere2_14ad28ee_07d7_48b8_9182_81e79b74e177(conn)), error = function(e) FAIL(e))
            PASS()
