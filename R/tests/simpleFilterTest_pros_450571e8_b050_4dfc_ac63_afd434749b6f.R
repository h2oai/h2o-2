            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_pros_450571e8_b050_4dfc_ac63_afd434749b6f <- function(conn) {
                Log.info("A munge-task R unit test on data <pros> testing the functional unit <>=> ")
                Log.info("Uploading pros")
                hex <- h2o.uploadFile(conn, "../../smalldata/logreg/umass_statdata/pros.dat", "pros.hex")
                Log.info("Filtering out rows by >= from dataset pros and column \"4\" using value 3.70480553605")
                     filterHex <- hex[hex[,c(4)] >= 3.70480553605,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"4" >= 3.70480553605,]
                Log.info("Filtering out rows by >= from dataset pros and column \"1\" using value 0.0445697876365")
                     filterHex <- hex[hex[,c(1)] >= 0.0445697876365,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 0.0445697876365,]
                Log.info("Filtering out rows by >= from dataset pros and column \"8\" using value 4.78772630383")
                     filterHex <- hex[hex[,c(8)] >= 4.78772630383,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"8" >= 4.78772630383,]
                Log.info("Filtering out rows by >= from dataset pros and column \"3\" using value 0.513018280615")
                     filterHex <- hex[hex[,c(3)] >= 0.513018280615,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"3" >= 0.513018280615,]
                Log.info("Filtering out rows by >= from dataset pros and column \"7\" using value 41.5242571321")
                     filterHex <- hex[hex[,c(7)] >= 41.5242571321,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"7" >= 41.5242571321,]
                    Log.info("Filtering out rows by >= from dataset pros and column \"2\" using value 57.627084865, and also subsetting columns.")
                     filterHex <- hex[hex[,c(2)] >= 57.627084865, c(2)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(2)] >= 57.627084865, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset pros and column \"3\" using value 0.365261877799, and also subsetting columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.365261877799, c(3)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.365261877799, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset pros and column \"4\" using value 2.20358739159, and also subsetting columns.")
                     filterHex <- hex[hex[,c(4)] >= 2.20358739159, c(4)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(4)] >= 2.20358739159, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset pros and column \"6\" using value 126.875631561, and also subsetting columns.")
                     filterHex <- hex[hex[,c(6)] >= 126.875631561, c(6)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(6)] >= 126.875631561, c(1,3,2,5,4,7,6,8)]
                    Log.info("Filtering out rows by >= from dataset pros and column \"7\" using value 0.769645036204, and also subsetting columns.")
                     filterHex <- hex[hex[,c(7)] >= 0.769645036204, c(7)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(7)] >= 0.769645036204, c(1,3,2,5,4,7,6,8)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data pros", simpleFilterTest_pros_450571e8_b050_4dfc_ac63_afd434749b6f(conn)), error = function(e) FAIL(e))
            PASS()
