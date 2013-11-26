            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_benign_d8ee5aaf_998a_45a7_b5de_13bf687fa575 <- function(conn) {
                Log.info("A munge-task R unit test on data <benign> testing the functional unit <>=> ")
                Log.info("Uploading benign")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/benign.xls", "benign.hex")
                Log.info("Filtering out rows by >= from dataset benign and column \"AGMT\" using value 47.0028301131")
                     filterHex <- hex[hex[,c("AGMT")] >= 47.0028301131,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"AGMT" >= 47.0028301131,]
                Log.info("Filtering out rows by >= from dataset benign and column \"WT\" using value 91.9768666957")
                     filterHex <- hex[hex[,c("WT")] >= 91.9768666957,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"WT" >= 91.9768666957,]
                Log.info("Filtering out rows by >= from dataset benign and column \"LIV\" using value 2.65834245075")
                     filterHex <- hex[hex[,c("LIV")] >= 2.65834245075,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"LIV" >= 2.65834245075,]
                Log.info("Filtering out rows by >= from dataset benign and column \"NLV\" using value 4.4758414329")
                     filterHex <- hex[hex[,c("NLV")] >= 4.4758414329,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"NLV" >= 4.4758414329,]
                Log.info("Filtering out rows by >= from dataset benign and column \"AGMT\" using value 35.0453297457")
                     filterHex <- hex[hex[,c("AGMT")] >= 35.0453297457,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"AGMT" >= 35.0453297457,]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGMN\" using value 16.3114467677, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGMN")] >= 16.3114467677, c("AGMN")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGMN")] >= 16.3114467677, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"DEG\" using value 1.41817143368, and also subsetting columns.")
                     filterHex <- hex[hex[,c("DEG")] >= 1.41817143368, c("DEG")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("DEG")] >= 1.41817143368, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"LIV\" using value 6.27074816008, and also subsetting columns.")
                     filterHex <- hex[hex[,c("LIV")] >= 6.27074816008, c("LIV")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("LIV")] >= 6.27074816008, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"STR\" using value 35.7403508406, and also subsetting columns.")
                     filterHex <- hex[hex[,c("STR")] >= 35.7403508406, c("STR")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("STR")] >= 35.7403508406, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
                    Log.info("Filtering out rows by >= from dataset benign and column \"AGMT\" using value 58.2408563178, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGMT")] >= 58.2408563178, c("AGMT")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGMT")] >= 58.2408563178, c("AGP1","MST","AGMT","DEG","HIGD","OBS","FNDX","WT","AGLP","LIV","NLV","AGMN","CHK","STR")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data benign", simpleFilterTest_benign_d8ee5aaf_998a_45a7_b5de_13bf687fa575(conn)), error = function(e) FAIL(e))
            PASS()
