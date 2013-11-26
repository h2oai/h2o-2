            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_parity_128_4_2_quad_c27cfb62_40da_4fb9_8306_6c626618f606 <- function(conn) {
                Log.info("A munge-task R unit test on data <parity_128_4_2_quad> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading parity_128_4_2_quad")
                hex <- h2o.uploadFile(conn, "../../../smalldata/parity_128_4_2_quad.data", "parity_128_4_2_quad.hex")
            Log.info("Performing compound task ( hex[,c(7)] != 5.0 ) | ( hex[,c(4)] < 10.0 ) & ( ( hex[,c(4)] < 10.0 ))  on dataset <parity_128_4_2_quad>")
                     filterHex <- hex[( hex[,c(7)] != 5.0 ) | ( hex[,c(4)] < 10.0 ) & ( ( hex[,c(4)] < 10.0 )) ,]
            Log.info("Performing compound task ( hex[,c(2)] != 0.0 ) | ( hex[,c(1)] < 10.0 ) & ( ( hex[,c(3)] < 5.0 ))  on dataset parity_128_4_2_quad, and also subsetting columns.")
                     filterHex <- hex[( hex[,c(2)] != 0.0 ) | ( hex[,c(1)] < 10.0 ) & ( ( hex[,c(3)] < 5.0 )) , c(1,3,2,5,4,7,6,8)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c(2)] != 0.0 ) | ( hex[,c(1)] < 10.0 ) & ( ( hex[,c(3)] < 5.0 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data parity_128_4_2_quad", complexFilterTest_parity_128_4_2_quad_c27cfb62_40da_4fb9_8306_6c626618f606(conn)), error = function(e) FAIL(e))
            PASS()
