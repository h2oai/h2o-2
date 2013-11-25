            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_two_spiral_8c936caa_334e_44a7_968b_6a9187e610ec <- function(conn) {
                Log.info("A munge-task R unit test on data <two_spiral> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading two_spiral")
                hex <- h2o.uploadFile(conn, "../../smalldata/neural/two_spiral.data", "two_spiral.hex")
            Log.info("Performing compound task ( hex[,c(\"Y\")] != -4.45168753003 ) | ( hex[,c(\"Y\")] < -1.16954969379 ) & ( ( hex[,c(\"Class\")] < 0.783088173736 ))  on dataset <two_spiral>")
                     filterHex <- hex[( hex[,c("Y")] != -4.45168753003 ) | ( hex[,c("Y")] < -1.16954969379 ) & ( ( hex[,c("Class")] < 0.783088173736 )) ,]
            Log.info("Performing compound task ( hex[,c(\"Class\")] != 0.0619462149364 ) | ( hex[,c(\"X\")] < 1.55766031148 ) & ( ( hex[,c(\"Class\")] < 0.266519116925 ))  on dataset two_spiral, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("Class")] != 0.0619462149364 ) | ( hex[,c("X")] < 1.55766031148 ) & ( ( hex[,c("Class")] < 0.266519116925 )) , c("Y","Class","X")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("Class")] != 0.0619462149364 ) | ( hex[,c("X")] < 1.55766031148 ) & ( ( hex[,c("Class")] < 0.266519116925 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data two_spiral", complexFilterTest_two_spiral_8c936caa_334e_44a7_968b_6a9187e610ec(conn)), error = function(e) FAIL(e))
            PASS()
