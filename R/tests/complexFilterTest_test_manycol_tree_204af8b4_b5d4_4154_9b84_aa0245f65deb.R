            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_manycol_tree_204af8b4_b5d4_4154_9b84_aa0245f65deb <- function(conn) {
                Log.info("A munge-task R unit test on data <test_manycol_tree> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading test_manycol_tree")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/test_manycol_tree.csv", "test_manycol_tree.hex")
            Log.info("Performing compound task !( ( ( hex[,c(\"X\")] != 1.66165091054 )) ) on dataset <test_manycol_tree>")
                     filterHex <- hex[!( ( ( hex[,c("X")] != 1.66165091054 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(\"A\")] != 2.09397687975 )) ) on dataset test_manycol_tree, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c("A")] != 2.09397687975 )) ), c("X","A","B","C","D")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c("A")] != 2.09397687975 )) ), c("response")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test_manycol_tree", complexFilterTest_test_manycol_tree_204af8b4_b5d4_4154_9b84_aa0245f65deb(conn)), error = function(e) FAIL(e))
            PASS()
