            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_tnc6_bd773876_b974_45a6_b0d7_a58b6369b0ce <- function(conn) {
                Log.info("A munge-task R unit test on data <tnc6> testing the functional unit <[> ")
                Log.info("Uploading tnc6")
                hex <- h2o.uploadFile(conn, "../../../smalldata/tnc6.csv", "tnc6.hex")
                Log.info("Performing a column slice of tnc6 using these columns: c(\"ticket\",\"boat\",\"cabin\",\"fare\",\"embarked\")")
                slicedHex <- hex[,c("ticket","boat","cabin","fare","embarked")]
                    Log.info("Performing a row slice of tnc6 using these rows: c(133,132,131,130,137,136,135,134,139,138,24,25,26,27,20,21,22,23,28,29,4,8,119,120,121,122,123,124,125,126,127,128,129,118,59,58,55,54,57,56,51,50,53,52,115,114,88,89,111,110,113,112,82,83,80,81,86,87,84,85,3,7,108,109,102,103,100,101,106,107,104,105,39,38,33,32,31,30,37,36,35,34,60,61,62,63,64,65,66,67,68,69,2,6,99,98,91,90,93,92,95,94,97,96,11,10,13,12,15,14,17,16,19,18,117,116,48,49,46,47,44,45,42,43,40,41,1,5,9,140,141,77,76,75,74,73,72,71,70,79,78)")
                slicedHex <- hex[c(133,132,131,130,137,136,135,134,139,138,24,25,26,27,20,21,22,23,28,29,4,8,119,120,121,122,123,124,125,126,127,128,129,118,59,58,55,54,57,56,51,50,53,52,115,114,88,89,111,110,113,112,82,83,80,81,86,87,84,85,3,7,108,109,102,103,100,101,106,107,104,105,39,38,33,32,31,30,37,36,35,34,60,61,62,63,64,65,66,67,68,69,2,6,99,98,91,90,93,92,95,94,97,96,11,10,13,12,15,14,17,16,19,18,117,116,48,49,46,47,44,45,42,43,40,41,1,5,9,140,141,77,76,75,74,73,72,71,70,79,78),]
                    Log.info("Performing a 1-by-1 column slice of tnc6 using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of tnc6 using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data tnc6", sliceTest_tnc6_bd773876_b974_45a6_b0d7_a58b6369b0ce(conn)), error = function(e) FAIL(e))
            PASS()
