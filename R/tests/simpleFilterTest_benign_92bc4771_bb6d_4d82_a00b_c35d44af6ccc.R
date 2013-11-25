            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_benign_92bc4771_bb6d_4d82_a00b_c35d44af6ccc <- function(conn) {
                Log.info("A munge-task R unit test on data <benign> testing the functional unit <>=> ")
                Log.info("Uploading benign")
                hex <- h2o.uploadFile(conn, "../../smalldata/logreg/benign.csv", "benign.hex")
                Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 36.708163147")
                     filterHex <- hex[hex[,c("STR")] >= 36.708163147,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"STR" >= 36.708163147,]
                Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 8.3524599182")
                     filterHex <- hex[hex[,c("STR")] >= 8.3524599182,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"STR" >= 8.3524599182,]
                Log.info("Filtering out rows by >= from dataset benign and column \"MST\" using value 1.26981097405")
                     filterHex <- hex[hex[,c("MST")] >= 1.26981097405,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"MST" >= 1.26981097405,]
                Log.info("Filtering out rows by >= from dataset benign and column \"OBS\" using value 1.55243587852")
                     filterHex <- hex[hex[,c("OBS")] >= 1.55243587852,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"OBS" >= 1.55243587852,]
                Log.info("Filtering out rows by >= from dataset benign and column \"LIV\" using value 4.43568334739")
                     filterHex <- hex[hex[,c("LIV")] >= 4.43568334739,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"LIV" >= 4.43568334739,]
                Log.info("Filtering out rows by >= from dataset benign and column \"WT\" using value 268.041451837")
                     filterHex <- hex[hex[,c("WT")] >= 268.041451837,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"WT" >= 268.041451837,]
                Log.info("Filtering out rows by >= from dataset benign and column \"HIGD\" using value 16.4500694317")
                     filterHex <- hex[hex[,c("HIGD")] >= 16.4500694317,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"HIGD" >= 16.4500694317,]
                Log.info("Filtering out rows by >= from dataset benign and column \"LIV\" using value 7.65796086343")
                     filterHex <- hex[hex[,c("LIV")] >= 7.65796086343,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"LIV" >= 7.65796086343,]
                Log.info("Filtering out rows by >= from dataset benign and column \"HIGD\" using value 10.3959709084")
                     filterHex <- hex[hex[,c("HIGD")] >= 10.3959709084,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"HIGD" >= 10.3959709084,]
                Log.info("Filtering out rows by >= from dataset benign and column \"CHK\" using value 1.06780633287")
                     filterHex <- hex[hex[,c("CHK")] >= 1.06780633287,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"CHK" >= 1.06780633287,]
                Log.info("Filtering out rows by >= from dataset benign and column \"AGLP\" using value 25.1286582395")
                     filterHex <- hex[hex[,c("AGLP")] >= 25.1286582395,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"AGLP" >= 25.1286582395,]
                Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 43.4949597641")
                     filterHex <- hex[hex[,c("STR")] >= 43.4949597641,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"STR" >= 43.4949597641,]
                Log.info("Filtering out rows by >= from dataset benign and column \"AGMN\" using value 16.4056640047")
                     filterHex <- hex[hex[,c("AGMN")] >= 16.4056640047,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"AGMN" >= 16.4056640047,]
                Log.info("Filtering out rows by >= from dataset benign and column \"WT\" using value 206.095968004")
                     filterHex <- hex[hex[,c("WT")] >= 206.095968004,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"WT" >= 206.095968004,]
                Log.info("Filtering out rows by >= from dataset benign and column \"AGMT\" using value 39.3172210763")
                     filterHex <- hex[hex[,c("AGMT")] >= 39.3172210763,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"AGMT" >= 39.3172210763,]
                Log.info("Filtering out rows by >= from dataset benign and column \"OBS\" using value 1.51804097485")
                     filterHex <- hex[hex[,c("OBS")] >= 1.51804097485,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"OBS" >= 1.51804097485,]
                    Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 27.7193178566, and also subsetting columns.")
                     filterHex <- hex[hex[,c("STR")] >= 27.7193178566, c("STR")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("STR")] >= 27.7193178566, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 1.95488487381, and also subsetting columns.")
                     filterHex <- hex[hex[,c("STR")] >= 1.95488487381, c("STR")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("STR")] >= 1.95488487381, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGLP\" using value 29.4445897775, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 29.4445897775, c("AGLP")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 29.4445897775, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"DEG\" using value 2.31226938599, and also subsetting columns.")
                     filterHex <- hex[hex[,c("DEG")] >= 2.31226938599, c("DEG")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("DEG")] >= 2.31226938599, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"OBS\" using value 2.71317354848, and also subsetting columns.")
                     filterHex <- hex[hex[,c("OBS")] >= 2.71317354848, c("OBS")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("OBS")] >= 2.71317354848, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGLP\" using value 39.2045998486, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 39.2045998486, c("AGLP")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 39.2045998486, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGLP\" using value 43.6476552761, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 43.6476552761, c("AGLP")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 43.6476552761, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGMN\" using value 13.8354874596, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGMN")] >= 13.8354874596, c("AGMN")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGMN")] >= 13.8354874596, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGLP\" using value 26.7897459342, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 26.7897459342, c("AGLP")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGLP")] >= 26.7897459342, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"CHK\" using value 1.08956940708, and also subsetting columns.")
                     filterHex <- hex[hex[,c("CHK")] >= 1.08956940708, c("CHK")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("CHK")] >= 1.08956940708, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 36.275180957, and also subsetting columns.")
                     filterHex <- hex[hex[,c("STR")] >= 36.275180957, c("STR")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("STR")] >= 36.275180957, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"NLV\" using value 4.40095507026, and also subsetting columns.")
                     filterHex <- hex[hex[,c("NLV")] >= 4.40095507026, c("NLV")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("NLV")] >= 4.40095507026, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 28.5177228475, and also subsetting columns.")
                     filterHex <- hex[hex[,c("STR")] >= 28.5177228475, c("STR")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("STR")] >= 28.5177228475, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGMN\" using value 14.6424796964, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGMN")] >= 14.6424796964, c("AGMN")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGMN")] >= 14.6424796964, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"NLV\" using value 5.42128007307, and also subsetting columns.")
                     filterHex <- hex[hex[,c("NLV")] >= 5.42128007307, c("NLV")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("NLV")] >= 5.42128007307, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"HIGD\" using value 14.4563558422, and also subsetting columns.")
                     filterHex <- hex[hex[,c("HIGD")] >= 14.4563558422, c("HIGD")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("HIGD")] >= 14.4563558422, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data benign", simpleFilterTest_benign_92bc4771_bb6d_4d82_a00b_c35d44af6ccc(conn)), error = function(e) FAIL(e))
            PASS()
