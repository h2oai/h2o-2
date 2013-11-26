            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test1_d6a3a843_498a_41b3_ab35_7c8c6b4dfd15 <- function(conn) {
                Log.info("A munge-task R unit test on data <test1> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading test1")
                hex <- h2o.uploadFile(conn, "../../../smalldata/test/test1.dat", "test1.hex")
            Log.info("Performing compound task ( hex[,c(4)] != 7.71554581245 ) | ( hex[,c(2)] == 3.10709674304 ) & ( hex[,c(1)] !| 2.72692901871 ) ! ( hex[,c(1)] > 16.4056410557 ) on dataset <test1>")
                     filterHex <- hex[( hex[,c(4)] != 7.71554581245 ) | ( hex[,c(2)] == 3.10709674304 ) & ( hex[,c(1)] !| 2.72692901871 ) ! ( hex[,c(1)] > 16.4056410557 ),]
            Log.info("Performing compound task ( hex[,c(4)] != 5.29547684045 ) | ( hex[,c(4)] == 5.84441452356 ) & ( hex[,c(1)] !| 2.96875495898 ) ! ( hex[,c(1)] > 2.05444112923 ) on dataset test1, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(4)] != 5.29547684045 ) | ( hex[,c(4)] == 5.84441452356 ) & ( hex[,c(1)] !| 2.96875495898 ) ! ( hex[,c(1)] > 2.05444112923 ), c(1,3,2,4)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(4)] != 5.29547684045 ) | ( hex[,c(4)] == 5.84441452356 ) & ( hex[,c(1)] !| 2.96875495898 ) ! ( hex[,c(1)] > 2.05444112923 ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test1", complexFilterTest_test1_d6a3a843_498a_41b3_ab35_7c8c6b4dfd15(conn)), error = function(e) FAIL(e))
            PASS()
