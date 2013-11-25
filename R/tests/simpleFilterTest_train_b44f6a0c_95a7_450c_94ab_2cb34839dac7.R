            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_train_b44f6a0c_95a7_450c_94ab_2cb34839dac7 <- function(conn) {
                Log.info("A munge-task R unit test on data <train> testing the functional unit <>=> ")
                Log.info("Uploading train")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_1x2x1000/h2o/train.csv", "train.hex")
                Log.info("Filtering out rows by >= from dataset train and column \"x\" using value 0.0334710707481")
                     filterHex <- hex[hex[,c("x")] >= 0.0334710707481,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"x" >= 0.0334710707481,]
                Log.info("Filtering out rows by >= from dataset train and column \"y\" using value 1.54561663824")
                     filterHex <- hex[hex[,c("y")] >= 1.54561663824,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"y" >= 1.54561663824,]
                Log.info("Filtering out rows by >= from dataset train and column \"x\" using value 0.872243475096")
                     filterHex <- hex[hex[,c("x")] >= 0.872243475096,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"x" >= 0.872243475096,]
                    Log.info("Filtering out rows by >= from dataset train and column \"x\" using value 0.057099320486, and also subsetting columns.")
                     filterHex <- hex[hex[,c("x")] >= 0.057099320486, c("x")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("x")] >= 0.057099320486, c("color","x","y")]
                    Log.info("Filtering out rows by >= from dataset train and column \"y\" using value 0.499305989679, and also subsetting columns.")
                     filterHex <- hex[hex[,c("y")] >= 0.499305989679, c("y")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("y")] >= 0.499305989679, c("color","x","y")]
                    Log.info("Filtering out rows by >= from dataset train and column \"x\" using value 0.321394725155, and also subsetting columns.")
                     filterHex <- hex[hex[,c("x")] >= 0.321394725155, c("x")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("x")] >= 0.321394725155, c("color","x","y")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data train", simpleFilterTest_train_b44f6a0c_95a7_450c_94ab_2cb34839dac7(conn)), error = function(e) FAIL(e))
            PASS()
