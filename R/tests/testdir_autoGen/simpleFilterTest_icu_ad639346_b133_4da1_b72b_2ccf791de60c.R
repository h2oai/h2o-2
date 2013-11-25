            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_icu_ad639346_b133_4da1_b72b_2ccf791de60c <- function(conn) {
                Log.info("A munge-task R unit test on data <icu> testing the functional unit <>=> ")
                Log.info("Uploading icu")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/umass_statdata/icu.dat", "icu.hex")
                Log.info("Filtering out rows by >= from dataset icu and column \"6\" using value 0.394191434211")
                     filterHex <- hex[hex[,c(6)] >= 0.394191434211,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"6" >= 0.394191434211,]
                Log.info("Filtering out rows by >= from dataset icu and column \"1\" using value 0.0659076130142")
                     filterHex <- hex[hex[,c(1)] >= 0.0659076130142,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 0.0659076130142,]
                Log.info("Filtering out rows by >= from dataset icu and column \"2\" using value 66.5572179763")
                     filterHex <- hex[hex[,c(2)] >= 66.5572179763,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"2" >= 66.5572179763,]
                Log.info("Filtering out rows by >= from dataset icu and column \"15\" using value 0.174831405796")
                     filterHex <- hex[hex[,c(15)] >= 0.174831405796,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"15" >= 0.174831405796,]
                Log.info("Filtering out rows by >= from dataset icu and column \"16\" using value 0.655491971711")
                     filterHex <- hex[hex[,c(16)] >= 0.655491971711,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"16" >= 0.655491971711,]
                Log.info("Filtering out rows by >= from dataset icu and column \"13\" using value 0.442811406343")
                     filterHex <- hex[hex[,c(13)] >= 0.442811406343,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"13" >= 0.442811406343,]
                Log.info("Filtering out rows by >= from dataset icu and column \"11\" using value 172.267961834")
                     filterHex <- hex[hex[,c(11)] >= 172.267961834,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"11" >= 172.267961834,]
                Log.info("Filtering out rows by >= from dataset icu and column \"16\" using value 0.0620562282195")
                     filterHex <- hex[hex[,c(16)] >= 0.0620562282195,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"16" >= 0.0620562282195,]
                Log.info("Filtering out rows by >= from dataset icu and column \"0\" using value 390.198864081")
                     filterHex <- hex[hex[,c(1)] >= 390.198864081,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 390.198864081,]
                Log.info("Filtering out rows by >= from dataset icu and column \"0\" using value 593.173169964")
                     filterHex <- hex[hex[,c(1)] >= 593.173169964,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 593.173169964,]
                Log.info("Filtering out rows by >= from dataset icu and column \"2\" using value 68.0344131119")
                     filterHex <- hex[hex[,c(2)] >= 68.0344131119,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"2" >= 68.0344131119,]
                Log.info("Filtering out rows by >= from dataset icu and column \"8\" using value 0.783436611153")
                     filterHex <- hex[hex[,c(8)] >= 0.783436611153,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"8" >= 0.783436611153,]
                Log.info("Filtering out rows by >= from dataset icu and column \"4\" using value 1.16581197936")
                     filterHex <- hex[hex[,c(4)] >= 1.16581197936,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"4" >= 1.16581197936,]
                Log.info("Filtering out rows by >= from dataset icu and column \"14\" using value 0.356435196321")
                     filterHex <- hex[hex[,c(14)] >= 0.356435196321,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"14" >= 0.356435196321,]
                Log.info("Filtering out rows by >= from dataset icu and column \"10\" using value 166.194771413")
                     filterHex <- hex[hex[,c(10)] >= 166.194771413,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"10" >= 166.194771413,]
                Log.info("Filtering out rows by >= from dataset icu and column \"0\" using value 628.986099183")
                     filterHex <- hex[hex[,c(1)] >= 628.986099183,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"0" >= 628.986099183,]
                Log.info("Filtering out rows by >= from dataset icu and column \"9\" using value 0.356308908613")
                     filterHex <- hex[hex[,c(9)] >= 0.356308908613,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"9" >= 0.356308908613,]
                Log.info("Filtering out rows by >= from dataset icu and column \"8\" using value 0.242390191742")
                     filterHex <- hex[hex[,c(8)] >= 0.242390191742,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"8" >= 0.242390191742,]
                Log.info("Filtering out rows by >= from dataset icu and column \"13\" using value 0.89400328678")
                     filterHex <- hex[hex[,c(13)] >= 0.89400328678,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"13" >= 0.89400328678,]
                Log.info("Filtering out rows by >= from dataset icu and column \"7\" using value 0.815139234983")
                     filterHex <- hex[hex[,c(7)] >= 0.815139234983,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"7" >= 0.815139234983,]
                Log.info("Filtering out rows by >= from dataset icu and column \"5\" using value 0.914738396674")
                     filterHex <- hex[hex[,c(5)] >= 0.914738396674,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"5" >= 0.914738396674,]
                Log.info("Filtering out rows by >= from dataset icu and column \"18\" using value 0.263278419189")
                     filterHex <- hex[hex[,c(18)] >= 0.263278419189,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"18" >= 0.263278419189,]
                Log.info("Filtering out rows by >= from dataset icu and column \"17\" using value 0.272609279376")
                     filterHex <- hex[hex[,c(17)] >= 0.272609279376,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"17" >= 0.272609279376,]
                Log.info("Filtering out rows by >= from dataset icu and column \"6\" using value 0.465171381692")
                     filterHex <- hex[hex[,c(6)] >= 0.465171381692,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"6" >= 0.465171381692,]
                    Log.info("Filtering out rows by >= from dataset icu and column \"0\" using value 407.156866697, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 407.156866697, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 407.156866697, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"9\" using value 0.461468505456, and also subsetting columns.")
                     filterHex <- hex[hex[,c(9)] >= 0.461468505456, c(9)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(9)] >= 0.461468505456, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"11\" using value 176.403618837, and also subsetting columns.")
                     filterHex <- hex[hex[,c(11)] >= 176.403618837, c(11)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(11)] >= 176.403618837, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"19\" using value 0.0635852289062, and also subsetting columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.0635852289062, c(19)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.0635852289062, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"15\" using value 0.40458701522, and also subsetting columns.")
                     filterHex <- hex[hex[,c(15)] >= 0.40458701522, c(15)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(15)] >= 0.40458701522, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"5\" using value 0.712338144039, and also subsetting columns.")
                     filterHex <- hex[hex[,c(5)] >= 0.712338144039, c(5)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(5)] >= 0.712338144039, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"11\" using value 182.797884692, and also subsetting columns.")
                     filterHex <- hex[hex[,c(11)] >= 182.797884692, c(11)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(11)] >= 182.797884692, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"0\" using value 736.101597957, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 736.101597957, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 736.101597957, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"3\" using value 0.00529171104629, and also subsetting columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.00529171104629, c(3)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(3)] >= 0.00529171104629, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"20\" using value 0.399055129827, and also subsetting columns.")
                     filterHex <- hex[hex[,c(20)] >= 0.399055129827, c(20)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(20)] >= 0.399055129827, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"1\" using value 0.969689772162, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.969689772162, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 0.969689772162, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"10\" using value 37.459214672, and also subsetting columns.")
                     filterHex <- hex[hex[,c(10)] >= 37.459214672, c(10)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(10)] >= 37.459214672, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"13\" using value 0.857329496869, and also subsetting columns.")
                     filterHex <- hex[hex[,c(13)] >= 0.857329496869, c(13)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(13)] >= 0.857329496869, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"13\" using value 0.787519750496, and also subsetting columns.")
                     filterHex <- hex[hex[,c(13)] >= 0.787519750496, c(13)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(13)] >= 0.787519750496, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"20\" using value 1.58112351371, and also subsetting columns.")
                     filterHex <- hex[hex[,c(20)] >= 1.58112351371, c(20)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(20)] >= 1.58112351371, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"20\" using value 0.595423359934, and also subsetting columns.")
                     filterHex <- hex[hex[,c(20)] >= 0.595423359934, c(20)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(20)] >= 0.595423359934, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"19\" using value 0.145795512414, and also subsetting columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.145795512414, c(19)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.145795512414, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"11\" using value 140.631756501, and also subsetting columns.")
                     filterHex <- hex[hex[,c(11)] >= 140.631756501, c(11)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(11)] >= 140.631756501, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"16\" using value 0.531542716334, and also subsetting columns.")
                     filterHex <- hex[hex[,c(16)] >= 0.531542716334, c(16)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(16)] >= 0.531542716334, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"6\" using value 0.724120545107, and also subsetting columns.")
                     filterHex <- hex[hex[,c(6)] >= 0.724120545107, c(6)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(6)] >= 0.724120545107, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"10\" using value 222.615011024, and also subsetting columns.")
                     filterHex <- hex[hex[,c(10)] >= 222.615011024, c(10)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(10)] >= 222.615011024, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"11\" using value 141.47136421, and also subsetting columns.")
                     filterHex <- hex[hex[,c(11)] >= 141.47136421, c(11)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(11)] >= 141.47136421, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"4\" using value 2.8862205519, and also subsetting columns.")
                     filterHex <- hex[hex[,c(4)] >= 2.8862205519, c(4)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(4)] >= 2.8862205519, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
                    Log.info("Filtering out rows by >= from dataset icu and column \"17\" using value 0.658514907112, and also subsetting columns.")
                     filterHex <- hex[hex[,c(17)] >= 0.658514907112, c(17)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(17)] >= 0.658514907112, c(11,10,13,12,15,14,17,16,19,18,20,1,3,2,5,4,7,6,9,8)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data icu", simpleFilterTest_icu_ad639346_b133_4da1_b72b_2ccf791de60c(conn)), error = function(e) FAIL(e))
            PASS()
