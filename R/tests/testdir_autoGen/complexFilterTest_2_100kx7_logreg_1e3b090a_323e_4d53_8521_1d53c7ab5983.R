            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_2_100kx7_logreg_1e3b090a_323e_4d53_8521_1d53c7ab5983 <- function(conn) {
                Log.info("A munge-task R unit test on data <2_100kx7_logreg> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading 2_100kx7_logreg")
                hex <- h2o.uploadFile(conn, "../../../smalldata/2_100kx7_logreg.data.gz", "2_100kx7_logreg.hex")
            Log.info("Performing compound task !( ( hex[,c(1)] <= 0.672826850886 ) | ( hex[,c(7)] == 0.0152460645125 ) & ( hex[,c(1)] <= 3.74865479017 ) & ( hex[,c(3)] <= 13.9596836438 ) & ( ( hex[,c(3)] <= 10.5156097385 )) ) on dataset <2_100kx7_logreg>")
                     filterHex <- hex[!( ( hex[,c(1)] <= 0.672826850886 ) | ( hex[,c(7)] == 0.0152460645125 ) & ( hex[,c(1)] <= 3.74865479017 ) & ( hex[,c(3)] <= 13.9596836438 ) & ( ( hex[,c(3)] <= 10.5156097385 )) ),]
            Log.info("Performing compound task !( ( hex[,c(6)] <= 129.452749633 ) | ( hex[,c(7)] == 0.503003725881 ) & ( hex[,c(7)] <= 0.910476115939 ) & ( hex[,c(5)] <= 0.354693731191 ) & ( ( hex[,c(4)] <= 7.07902241051 )) ) on dataset 2_100kx7_logreg, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c(6)] <= 129.452749633 ) | ( hex[,c(7)] == 0.503003725881 ) & ( hex[,c(7)] <= 0.910476115939 ) & ( hex[,c(5)] <= 0.354693731191 ) & ( ( hex[,c(4)] <= 7.07902241051 )) ), c(5,4,7,6)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c(6)] <= 129.452749633 ) | ( hex[,c(7)] == 0.503003725881 ) & ( hex[,c(7)] <= 0.910476115939 ) & ( hex[,c(5)] <= 0.354693731191 ) & ( ( hex[,c(4)] <= 7.07902241051 )) ), c(1,3,2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data 2_100kx7_logreg", complexFilterTest_2_100kx7_logreg_1e3b090a_323e_4d53_8521_1d53c7ab5983(conn)), error = function(e) FAIL(e))
            PASS()
