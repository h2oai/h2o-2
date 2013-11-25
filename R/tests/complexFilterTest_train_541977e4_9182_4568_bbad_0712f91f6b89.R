            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_train_541977e4_9182_4568_bbad_0712f91f6b89 <- function(conn) {
                Log.info("A munge-task R unit test on data <train> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading train")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x200/weka/train.csv.arff", "train.hex")
            Log.info("Performing compound task ( hex[,c(1)] != 1.80003941188 ) | ( hex[,c(1)] == 0.23380398118 ) & ( hex[,c(1)] !| 0.686464849157 ) ! ( hex[,c(1)] > 1.80039855541 ) on dataset <train>")
                     filterHex <- hex[( hex[,c(1)] != 1.80003941188 ) | ( hex[,c(1)] == 0.23380398118 ) & ( hex[,c(1)] !| 0.686464849157 ) ! ( hex[,c(1)] > 1.80039855541 ),]
            Log.info("Performing compound task ( hex[,c(1)] != 1.97803431948 ) | ( hex[,c(1)] == 1.93196099667 ) & ( hex[,c(1)] !| 1.70466170344 ) ! ( hex[,c(1)] > 1.25777396022 ) on dataset train, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(1)] != 1.97803431948 ) | ( hex[,c(1)] == 1.93196099667 ) & ( hex[,c(1)] !| 1.70466170344 ) ! ( hex[,c(1)] > 1.25777396022 ), c(1)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(1)] != 1.97803431948 ) | ( hex[,c(1)] == 1.93196099667 ) & ( hex[,c(1)] !| 1.70466170344 ) ! ( hex[,c(1)] > 1.25777396022 ), c(2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data train", complexFilterTest_train_541977e4_9182_4568_bbad_0712f91f6b89(conn)), error = function(e) FAIL(e))
            PASS()
