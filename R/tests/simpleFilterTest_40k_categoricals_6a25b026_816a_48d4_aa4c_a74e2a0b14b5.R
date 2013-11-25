            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_40k_categoricals_6a25b026_816a_48d4_aa4c_a74e2a0b14b5 <- function(conn) {
                Log.info("A munge-task R unit test on data <40k_categoricals> testing the functional unit <>=> ")
                Log.info("Uploading 40k_categoricals")
                hex <- h2o.uploadFile(conn, "../../smalldata/categoricals/40k_categoricals.csv.gz", "40k_categoricals.hex")
                Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 5517.67074812")
                     filterHex <- hex[hex[,c(1)] >= 5517.67074812,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 5517.67074812,]
                Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"3\" using value 0.0")
                     filterHex <- hex[hex[,c(3)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"3" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 26662.806346")
                     filterHex <- hex[hex[,c(1)] >= 26662.806346,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 26662.806346,]
                Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"3\" using value 0.0")
                     filterHex <- hex[hex[,c(3)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"3" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 14808.5559385")
                     filterHex <- hex[hex[,c(1)] >= 14808.5559385,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 14808.5559385,]
                Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"3\" using value 0.0")
                     filterHex <- hex[hex[,c(3)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"3" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"3\" using value 0.0")
                     filterHex <- hex[hex[,c(3)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"3" >= 0.0,]
                    Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 13557.5037851, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 13557.5037851, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 13557.5037851, c(1,3,2)]
                    Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 11130.4577575, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 11130.4577575, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 11130.4577575, c(1,3,2)]
                    Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 2794.78508979, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 2794.78508979, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 2794.78508979, c(1,3,2)]
                    Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 10810.7345212, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 10810.7345212, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 10810.7345212, c(1,3,2)]
                    Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"3\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.0, c(3)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.0, c(1,3,2)]
                    Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"1\" using value 19834.479752, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 19834.479752, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 19834.479752, c(1,3,2)]
                    Log.info("Filtering out rows by >= from dataset 40k_categoricals and column \"3\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.0, c(3)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.0, c(1,3,2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data 40k_categoricals", simpleFilterTest_40k_categoricals_6a25b026_816a_48d4_aa4c_a74e2a0b14b5(conn)), error = function(e) FAIL(e))
            PASS()
