            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_tnc6_3b02ae2e_f90f_4b7b_940e_ae928162c88d <- function(conn) {
                Log.info("A munge-task R unit test on data <tnc6> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading tnc6")
                hex <- h2o.uploadFile(conn, "../../smalldata/tnc6.csv", "tnc6.hex")
            Log.info("Performing compound task ( hex[,c(\"cabin\")] != -1.0 ) | ( hex[,c(\"cabin\")] == -1.0 ) & ( hex[,c(\"ticket\")] !| 1700840.83592 ) ! ( hex[,c(\"ticket\")] > 540258.023085 ) on dataset <tnc6>")
                     filterHex <- hex[( hex[,c("cabin")] != -1.0 ) | ( hex[,c("cabin")] == -1.0 ) & ( hex[,c("ticket")] !| 1700840.83592 ) ! ( hex[,c("ticket")] > 540258.023085 ),]
            Log.info("Performing compound task ( hex[,c(\"home.dest\")] != -1.0 ) | ( hex[,c(\"cabin\")] == -1.0 ) & ( hex[,c(\"survived\")] !| 0.472822899846 ) ! ( hex[,c(\"embarked\")] > -1.0 ) on dataset tnc6, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("home.dest")] != -1.0 ) | ( hex[,c("cabin")] == -1.0 ) & ( hex[,c("survived")] !| 0.472822899846 ) ! ( hex[,c("embarked")] > -1.0 ), c("boat","fare","ticket","survived","cabin","home.dest","body","embarked")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("home.dest")] != -1.0 ) | ( hex[,c("cabin")] == -1.0 ) & ( hex[,c("survived")] !| 0.472822899846 ) ! ( hex[,c("embarked")] > -1.0 ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data tnc6", complexFilterTest_tnc6_3b02ae2e_f90f_4b7b_940e_ae928162c88d(conn)), error = function(e) FAIL(e))
            PASS()
