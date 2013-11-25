            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_test_tree_1fb34ca1_68db_444e_aa0a_9dd3756d454c <- function(conn) {
                Log.info("A munge-task R unit test on data <test_tree> testing the functional unit <>=> ")
                Log.info("Uploading test_tree")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/test_tree.csv", "test_tree.hex")
                Log.info("Filtering out rows by >= from dataset test_tree and column \"X\" using value 1.62029753798")
                     filterHex <- hex[hex[,c("X")] >= 1.62029753798,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"X" >= 1.62029753798,]
                Log.info("Filtering out rows by >= from dataset test_tree and column \"X\" using value 2.00291237686")
                     filterHex <- hex[hex[,c("X")] >= 2.00291237686,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"X" >= 2.00291237686,]
                    Log.info("Filtering out rows by >= from dataset test_tree and column \"Y\" using value 0.812256355739, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Y")] >= 0.812256355739, c("Y")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Y")] >= 0.812256355739, c("X","Y")]
                    Log.info("Filtering out rows by >= from dataset test_tree and column \"X\" using value 2.78479471198, and also subsetting columns.")
                     filterHex <- hex[hex[,c("X")] >= 2.78479471198, c("X")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("X")] >= 2.78479471198, c("X","Y")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data test_tree", simpleFilterTest_test_tree_1fb34ca1_68db_444e_aa0a_9dd3756d454c(conn)), error = function(e) FAIL(e))
            PASS()
