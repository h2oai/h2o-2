            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_badchars_d975699e_f42c_43ab_8c68_9d12beff525e <- function(conn) {
                Log.info("A munge-task R unit test on data <badchars> testing the functional unit <>=> ")
                Log.info("Uploading badchars")
                hex <- h2o.uploadFile(conn, "../../smalldata/badchars.csv", "badchars.hex")
                Log.info("Filtering out rows by >= from dataset badchars and column \"11\" using value 0.0")
                     filterHex <- hex[hex[,c(11)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"11" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"27\" using value 0.0")
                     filterHex <- hex[hex[,c(27)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"27" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"39\" using value 0.0")
                     filterHex <- hex[hex[,c(39)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"39" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"28\" using value 0.0")
                     filterHex <- hex[hex[,c(28)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"28" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"15\" using value 0.0")
                     filterHex <- hex[hex[,c(15)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"15" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"9\" using value 3867.35669517")
                     filterHex <- hex[hex[,c(9)] >= 3867.35669517,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"9" >= 3867.35669517,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"28\" using value 0.0")
                     filterHex <- hex[hex[,c(28)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"28" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"1\" using value 208.349199326")
                     filterHex <- hex[hex[,c(1)] >= 208.349199326,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"1" >= 208.349199326,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"11\" using value 0.0")
                     filterHex <- hex[hex[,c(11)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"11" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"53\" using value 0.0")
                     filterHex <- hex[hex[,c(53)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"53" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"7\" using value 243.202592853")
                     filterHex <- hex[hex[,c(7)] >= 243.202592853,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"7" >= 243.202592853,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"17\" using value 0.0")
                     filterHex <- hex[hex[,c(17)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"17" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"9\" using value 4713.79896676")
                     filterHex <- hex[hex[,c(9)] >= 4713.79896676,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"9" >= 4713.79896676,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"15\" using value 0.0")
                     filterHex <- hex[hex[,c(15)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"15" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"24\" using value 0.0")
                     filterHex <- hex[hex[,c(24)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"24" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"11\" using value 0.0")
                     filterHex <- hex[hex[,c(11)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"11" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"32\" using value 0.0")
                     filterHex <- hex[hex[,c(32)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"32" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"35\" using value 0.56400570033")
                     filterHex <- hex[hex[,c(35)] >= 0.56400570033,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"35" >= 0.56400570033,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"24\" using value 0.0")
                     filterHex <- hex[hex[,c(24)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"24" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"15\" using value 0.0")
                     filterHex <- hex[hex[,c(15)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"15" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"39\" using value 0.0")
                     filterHex <- hex[hex[,c(39)] >= 0.0,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"39" >= 0.0,]
                Log.info("Filtering out rows by >= from dataset badchars and column \"54\" using value 5.57307990608")
                     filterHex <- hex[hex[,c(54)] >= 5.57307990608,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"54" >= 5.57307990608,]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"24\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(24)] >= 0.0, c(24)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(24)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"41\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(41)] >= 0.0, c(41)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(41)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"52\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(52)] >= 0.0, c(52)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(52)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"24\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(24)] >= 0.0, c(24)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(24)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"9\" using value 2496.50994872, and also subsetting columns.")
                     filterHex <- hex[hex[,c(9)] >= 2496.50994872, c(9)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(9)] >= 2496.50994872, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"4\" using value 95.7671907437, and also subsetting columns.")
                     filterHex <- hex[hex[,c(4)] >= 95.7671907437, c(4)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(4)] >= 95.7671907437, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"43\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(43)] >= 0.0, c(43)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(43)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"22\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(22)] >= 0.0, c(22)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(22)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"28\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(28)] >= 0.0, c(28)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(28)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"19\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.0, c(19)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"26\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(26)] >= 0.0, c(26)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(26)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"19\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.0, c(19)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(19)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"28\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(28)] >= 0.0, c(28)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(28)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"53\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(53)] >= 0.0, c(53)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(53)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"40\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(40)] >= 0.0, c(40)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(40)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"29\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(29)] >= 0.0, c(29)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(29)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"20\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(20)] >= 0.0, c(20)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(20)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"16\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(16)] >= 0.0, c(16)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(16)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"47\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(47)] >= 0.0, c(47)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(47)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"51\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(51)] >= 0.0, c(51)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(51)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"1\" using value 167.845016018, and also subsetting columns.")
                     filterHex <- hex[hex[,c(1)] >= 167.845016018, c(1)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(1)] >= 167.845016018, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
                    Log.info("Filtering out rows by >= from dataset badchars and column \"51\" using value 0.0, and also subsetting columns.")
                     filterHex <- hex[hex[,c(51)] >= 0.0, c(51)]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c(51)] >= 0.0, c(54,42,48,43,49,52,53,24,25,26,27,20,21,22,23,46,47,44,45,28,29,40,41,1,3,2,5,4,7,6,9,8,51,39,38,11,10,13,12,15,14,17,16,19,18,31,30,37,36,35,34,33,32,50)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data badchars", simpleFilterTest_badchars_d975699e_f42c_43ab_8c68_9d12beff525e(conn)), error = function(e) FAIL(e))
            PASS()
