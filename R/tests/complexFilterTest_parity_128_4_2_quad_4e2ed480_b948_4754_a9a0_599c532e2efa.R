            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_parity_128_4_2_quad_4e2ed480_b948_4754_a9a0_599c532e2efa <- function(conn) {
                Log.info("A munge-task R unit test on data <parity_128_4_2_quad> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading parity_128_4_2_quad")
                hex <- h2o.uploadFile(conn, "../../smalldata/parity_128_4_2_quad.data", "parity_128_4_2_quad.hex")
            Log.info("Performing compound task ( hex[,c(4)] != 10.0 ) | ( hex[,c(1)] == 1.25965294004 ) & ( hex[,c(6)] !| 0.0 ) ! ( hex[,c(6)] > 0.0 ) on dataset <parity_128_4_2_quad>")
                     filterHex <- hex[( hex[,c(4)] != 10.0 ) | ( hex[,c(1)] == 1.25965294004 ) & ( hex[,c(6)] !| 0.0 ) ! ( hex[,c(6)] > 0.0 ),]
            Log.info("Performing compound task ( hex[,c(5)] != 0.339868725526 ) | ( hex[,c(5)] == 0.17891374351 ) & ( hex[,c(5)] !| 1.37418195286 ) ! ( hex[,c(3)] > 5.0 ) on dataset parity_128_4_2_quad, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(5)] != 0.339868725526 ) | ( hex[,c(5)] == 0.17891374351 ) & ( hex[,c(5)] !| 1.37418195286 ) ! ( hex[,c(3)] > 5.0 ), c(3,2,5,4,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(5)] != 0.339868725526 ) | ( hex[,c(5)] == 0.17891374351 ) & ( hex[,c(5)] !| 1.37418195286 ) ! ( hex[,c(3)] > 5.0 ), c(1,7)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data parity_128_4_2_quad", complexFilterTest_parity_128_4_2_quad_4e2ed480_b948_4754_a9a0_599c532e2efa(conn)), error = function(e) FAIL(e))
            PASS()
