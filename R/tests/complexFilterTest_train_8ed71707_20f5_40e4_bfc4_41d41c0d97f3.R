            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_train_8ed71707_20f5_40e4_bfc4_41d41c0d97f3 <- function(conn) {
                Log.info("A munge-task R unit test on data <train> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading train")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x500/h2o/train.csv", "train.hex")
            Log.info("Performing compound task !( ( hex[,c(\"x\")] <= 1.03766176378 ) | ( hex[,c(\"x\")] == 0.313625044119 ) & ( hex[,c(\"y\")] <= 1.6323621256 ) & ( hex[,c(\"x\")] <= 1.25111683208 ) & ( ( hex[,c(\"y\")] <= 1.00599060264 )) ) on dataset <train>")
                     filterHex <- hex[!( ( hex[,c("x")] <= 1.03766176378 ) | ( hex[,c("x")] == 0.313625044119 ) & ( hex[,c("y")] <= 1.6323621256 ) & ( hex[,c("x")] <= 1.25111683208 ) & ( ( hex[,c("y")] <= 1.00599060264 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"y\")] <= 0.934969227113 ) | ( hex[,c(\"x\")] == 1.66269797614 ) & ( hex[,c(\"y\")] <= 1.66201281113 ) & ( hex[,c(\"x\")] <= 0.403470676541 ) & ( ( hex[,c(\"x\")] <= 1.80909137816 )) ) on dataset train, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("y")] <= 0.934969227113 ) | ( hex[,c("x")] == 1.66269797614 ) & ( hex[,c("y")] <= 1.66201281113 ) & ( hex[,c("x")] <= 0.403470676541 ) & ( ( hex[,c("x")] <= 1.80909137816 )) ), c("x","y")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("y")] <= 0.934969227113 ) | ( hex[,c("x")] == 1.66269797614 ) & ( hex[,c("y")] <= 1.66201281113 ) & ( hex[,c("x")] <= 0.403470676541 ) & ( ( hex[,c("x")] <= 1.80909137816 )) ), c("color")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data train", complexFilterTest_train_8ed71707_20f5_40e4_bfc4_41d41c0d97f3(conn)), error = function(e) FAIL(e))
            PASS()
