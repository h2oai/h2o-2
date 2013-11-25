            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_test_30e59526_2e25_4bb5_a3ed_e7687b89151a <- function(conn) {
                Log.info("A munge-task R unit test on data <test> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading test")
                hex <- h2o.uploadFile(conn, "../../smalldata/chess/chess_2x2x500/h2o/test.csv", "test.hex")
            Log.info("Performing compound task !( ( hex[,c(\"x\")] <= 0.958005885451 ) | ( hex[,c(\"x\")] == 0.342748905438 ) & ( hex[,c(\"x\")] <= 1.2746437409 ) & ( hex[,c(\"y\")] <= 0.0271364789795 ) & ( ( hex[,c(\"x\")] <= 1.80281803658 )) ) on dataset <test>")
                     filterHex <- hex[!( ( hex[,c("x")] <= 0.958005885451 ) | ( hex[,c("x")] == 0.342748905438 ) & ( hex[,c("x")] <= 1.2746437409 ) & ( hex[,c("y")] <= 0.0271364789795 ) & ( ( hex[,c("x")] <= 1.80281803658 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"x\")] <= 1.17938210648 ) | ( hex[,c(\"y\")] == 1.96452988447 ) & ( hex[,c(\"x\")] <= 0.447215939086 ) & ( hex[,c(\"x\")] <= 0.997959954686 ) & ( ( hex[,c(\"x\")] <= 0.420849774469 )) ) on dataset test, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("x")] <= 1.17938210648 ) | ( hex[,c("y")] == 1.96452988447 ) & ( hex[,c("x")] <= 0.447215939086 ) & ( hex[,c("x")] <= 0.997959954686 ) & ( ( hex[,c("x")] <= 0.420849774469 )) ), c("x","y")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("x")] <= 1.17938210648 ) | ( hex[,c("y")] == 1.96452988447 ) & ( hex[,c("x")] <= 0.447215939086 ) & ( hex[,c("x")] <= 0.997959954686 ) & ( ( hex[,c("x")] <= 0.420849774469 )) ), c("color")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data test", complexFilterTest_test_30e59526_2e25_4bb5_a3ed_e7687b89151a(conn)), error = function(e) FAIL(e))
            PASS()
