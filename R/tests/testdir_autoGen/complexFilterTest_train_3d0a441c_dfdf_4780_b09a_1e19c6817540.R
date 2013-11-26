            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_train_3d0a441c_dfdf_4780_b09a_1e19c6817540 <- function(conn) {
                Log.info("A munge-task R unit test on data <train> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading train")
                hex <- h2o.uploadFile(conn, "../../../smalldata/chess/chess_2x2x500/h2o/train.csv", "train.hex")
            Log.info("Performing compound task !( ( ( hex[,c(\"x\")] != 1.3948227204 )) ) on dataset <train>")
                     filterHex <- hex[!( ( ( hex[,c("x")] != 1.3948227204 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(\"y\")] != 0.200856484709 )) ) on dataset train, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c("y")] != 0.200856484709 )) ), c("x","y")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c("y")] != 0.200856484709 )) ), c("color")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data train", complexFilterTest_train_3d0a441c_dfdf_4780_b09a_1e19c6817540(conn)), error = function(e) FAIL(e))
            PASS()
