            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_allyears2k_a15736c0_0ca7_4b1b_9a12_7f37d7ce250d <- function(conn) {
                Log.info("A munge-task R unit test on data <allyears2k> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading allyears2k")
                hex <- h2o.uploadFile(conn, "../../smalldata/airlines/allyears2k.zip", "allyears2k.hex")
            Log.info("Performing compound task ( hex[,c(2)] != 6.90065475459 ) | ( hex[,c(14)] == 172.079419147 ) & ( hex[,c(20)] !| 0.155299061352 ) ! ( hex[,c(27)] > 12.8028555623 ) on dataset <allyears2k>")
                     filterHex <- hex[( hex[,c(2)] != 6.90065475459 ) | ( hex[,c(14)] == 172.079419147 ) & ( hex[,c(20)] !| 0.155299061352 ) ! ( hex[,c(27)] > 12.8028555623 ),]
            Log.info("Performing compound task ( hex[,c(10)] != 30.9048046036 ) | ( hex[,c(21)] == 0.200277696014 ) & ( hex[,c(24)] !| 135.835913504 ) ! ( hex[,c(23)] > 0.641467608756 ) on dataset allyears2k, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(10)] != 30.9048046036 ) | ( hex[,c(21)] == 0.200277696014 ) & ( hex[,c(24)] !| 135.835913504 ) ! ( hex[,c(23)] > 0.641467608756 ), c(24,10,21,23,19,28,5,7)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(10)] != 30.9048046036 ) | ( hex[,c(21)] == 0.200277696014 ) & ( hex[,c(24)] !| 135.835913504 ) ! ( hex[,c(23)] > 0.641467608756 ), c(25,26,27,20,22,29,1,3,2,4,6,9,8,11,13,12,15,14,17,16,18,30)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data allyears2k", complexFilterTest_allyears2k_a15736c0_0ca7_4b1b_9a12_7f37d7ce250d(conn)), error = function(e) FAIL(e))
            PASS()
