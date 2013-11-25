            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_constantColumn_22aaa36a_3fda_4fd7_83c1_a741d2c3959b <- function(conn) {
                Log.info("A munge-task R unit test on data <constantColumn> testing the functional unit <>=> ")
                Log.info("Uploading constantColumn")
                hex <- h2o.uploadFile(conn, "../../smalldata/constantColumn.csv", "constantColumn.hex")
                Log.info("Filtering out rows by >= from dataset constantColumn and column \"konstant\" using value 0.1")
                     filterHex <- hex[hex[,c("konstant")] >= 0.1,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"konstant" >= 0.1,]
                    Log.info("Filtering out rows by >= from dataset constantColumn and column \"konstant\" using value 0.1, and also subsetting columns.")
                     filterHex <- hex[hex[,c("konstant")] >= 0.1, c("konstant")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("konstant")] >= 0.1, c("konstant")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data constantColumn", simpleFilterTest_constantColumn_22aaa36a_3fda_4fd7_83c1_a741d2c3959b(conn)), error = function(e) FAIL(e))
            PASS()
