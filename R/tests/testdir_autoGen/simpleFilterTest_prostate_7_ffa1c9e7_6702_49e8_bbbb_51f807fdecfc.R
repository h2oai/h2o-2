            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_prostate_7_ffa1c9e7_6702_49e8_bbbb_51f807fdecfc <- function(conn) {
                Log.info("A munge-task R unit test on data <prostate_7> testing the functional unit <>=> ")
                Log.info("Uploading prostate_7")
                hex <- h2o.uploadFile(conn, "../../../smalldata/parse_folder_test/prostate_7.csv", "prostate_7.hex")
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"4\" using value 1.26500151738")
                     filterHex <- hex[hex[,c(4)] >= 1.26500151738,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"4" >= 1.26500151738,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"4\" using value 1.01752851228")
                     filterHex <- hex[hex[,c(4)] >= 1.01752851228,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"4" >= 1.01752851228,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"4\" using value 3.43350650479")
                     filterHex <- hex[hex[,c(4)] >= 3.43350650479,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"4" >= 3.43350650479,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"8\" using value 8.78886190182")
                     filterHex <- hex[hex[,c(8)] >= 8.78886190182,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"8" >= 8.78886190182,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"6\" using value 57.8662133755")
                     filterHex <- hex[hex[,c(6)] >= 57.8662133755,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"6" >= 57.8662133755,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"8\" using value 6.37072472581")
                     filterHex <- hex[hex[,c(8)] >= 6.37072472581,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"8" >= 6.37072472581,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"7\" using value 50.8928656334")
                     filterHex <- hex[hex[,c(7)] >= 50.8928656334,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"7" >= 50.8928656334,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"4\" using value 3.07442676389")
                     filterHex <- hex[hex[,c(4)] >= 3.07442676389,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"4" >= 3.07442676389,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"0\" using value 272.636464194")
                     filterHex <- hex[hex[,c(1)] >= 272.636464194,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 272.636464194,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"2\" using value 58.6678347669")
                     filterHex <- hex[hex[,c(2)] >= 58.6678347669,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"2" >= 58.6678347669,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"1\" using value 0.42196759576")
                     filterHex <- hex[hex[,c(1)] >= 0.42196759576,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 0.42196759576,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"0\" using value 265.467431488")
                     filterHex <- hex[hex[,c(1)] >= 265.467431488,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 265.467431488,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"0\" using value 274.688215309")
                     filterHex <- hex[hex[,c(1)] >= 274.688215309,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 274.688215309,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"0\" using value 232.651610501")
                     filterHex <- hex[hex[,c(1)] >= 232.651610501,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 232.651610501,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"2\" using value 51.4293348668")
                     filterHex <- hex[hex[,c(2)] >= 51.4293348668,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"2" >= 51.4293348668,]
                Log.info("Filtering out rows by >= from dataset prostate_7 and column \"0\" using value 249.5360918")
                     filterHex <- hex[hex[,c(1)] >= 249.5360918,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 249.5360918,]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"7\" using value 28.4297317629, and also subsetting columns.")
                     filterHex <- hex[hex[,c(7)] >= 28.4297317629, c(7)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(7)] >= 28.4297317629, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"4\" using value 1.85524140481, and also subsetting columns.")
                     filterHex <- hex[hex[,c(4)] >= 1.85524140481, c(4)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(4)] >= 1.85524140481, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"6\" using value 51.905988574, and also subsetting columns.")
                     filterHex <- hex[hex[,c(6)] >= 51.905988574, c(6)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(6)] >= 51.905988574, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"7\" using value 49.1103317562, and also subsetting columns.")
                     filterHex <- hex[hex[,c(7)] >= 49.1103317562, c(7)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(7)] >= 49.1103317562, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"5\" using value 1.29400380267, and also subsetting columns.")
                     filterHex <- hex[hex[,c(5)] >= 1.29400380267, c(5)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(5)] >= 1.29400380267, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"4\" using value 1.70413701273, and also subsetting columns.")
                     filterHex <- hex[hex[,c(4)] >= 1.70413701273, c(4)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(4)] >= 1.70413701273, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"6\" using value 49.550005469, and also subsetting columns.")
                     filterHex <- hex[hex[,c(6)] >= 49.550005469, c(6)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(6)] >= 49.550005469, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"6\" using value 88.1335484149, and also subsetting columns.")
                     filterHex <- hex[hex[,c(6)] >= 88.1335484149, c(6)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(6)] >= 88.1335484149, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"0\" using value 258.487930278, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 258.487930278, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 258.487930278, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"5\" using value 1.48735509423, and also subsetting columns.")
                     filterHex <- hex[hex[,c(5)] >= 1.48735509423, c(5)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(5)] >= 1.48735509423, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"8\" using value 5.29582790058, and also subsetting columns.")
                     filterHex <- hex[hex[,c(8)] >= 5.29582790058, c(8)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(8)] >= 5.29582790058, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"1\" using value 0.624393380354, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.624393380354, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.624393380354, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"1\" using value 0.469127546968, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.469127546968, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.469127546968, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"1\" using value 0.778919246765, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.778919246765, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.778919246765, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"8\" using value 8.94650264353, and also subsetting columns.")
                     filterHex <- hex[hex[,c(8)] >= 8.94650264353, c(8)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(8)] >= 8.94650264353, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset prostate_7 and column \"4\" using value 2.05642680239, and also subsetting columns.")
                     filterHex <- hex[hex[,c(4)] >= 2.05642680239, c(4)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(4)] >= 2.05642680239, c(1,3,2,5,4,7,6,8)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data prostate_7", simpleFilterTest_prostate_7_ffa1c9e7_6702_49e8_bbbb_51f807fdecfc(conn)), error = function(e) FAIL(e))
            PASS()
