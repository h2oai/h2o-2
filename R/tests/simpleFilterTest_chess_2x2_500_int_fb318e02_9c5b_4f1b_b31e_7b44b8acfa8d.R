            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_chess_2x2_500_int_fb318e02_9c5b_4f1b_b31e_7b44b8acfa8d <- function(conn) {
                Log.info("A munge-task R unit test on data <chess_2x2_500_int> testing the functional unit <>=> ")
                Log.info("Uploading chess_2x2_500_int")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x500/h2o/chess_2x2_500_int.csv", "chess_2x2_500_int.hex")
                Log.info("Filtering out rows by >= from dataset chess_2x2_500_int and column \"y\" using value 1914206.35905")
                     filterHex <- hex[hex[,c("y")] >= 1914206.35905,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"y" >= 1914206.35905,]
                Log.info("Filtering out rows by >= from dataset chess_2x2_500_int and column \"x\" using value 342257.395966")
                     filterHex <- hex[hex[,c("x")] >= 342257.395966,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"x" >= 342257.395966,]
                Log.info("Filtering out rows by >= from dataset chess_2x2_500_int and column \"x\" using value 903917.292278")
                     filterHex <- hex[hex[,c("x")] >= 903917.292278,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"x" >= 903917.292278,]
                    Log.info("Filtering out rows by >= from dataset chess_2x2_500_int and column \"x\" using value 457570.211029, and also subsetting columns.")
                     filterHex <- hex[hex[,c("x")] >= 457570.211029, c("x")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("x")] >= 457570.211029, c("color","x","y")]
                    Log.info("Filtering out rows by >= from dataset chess_2x2_500_int and column \"y\" using value 239444.333546, and also subsetting columns.")
                     filterHex <- hex[hex[,c("y")] >= 239444.333546, c("y")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("y")] >= 239444.333546, c("color","x","y")]
                    Log.info("Filtering out rows by >= from dataset chess_2x2_500_int and column \"y\" using value 296977.46557, and also subsetting columns.")
                     filterHex <- hex[hex[,c("y")] >= 296977.46557, c("y")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("y")] >= 296977.46557, c("color","x","y")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data chess_2x2_500_int", simpleFilterTest_chess_2x2_500_int_fb318e02_9c5b_4f1b_b31e_7b44b8acfa8d(conn)), error = function(e) FAIL(e))
            PASS()
