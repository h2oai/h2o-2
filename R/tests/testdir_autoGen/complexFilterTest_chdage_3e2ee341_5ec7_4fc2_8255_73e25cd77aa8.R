            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_chdage_3e2ee341_5ec7_4fc2_8255_73e25cd77aa8 <- function(conn) {
                Log.info("A munge-task R unit test on data <chdage> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading chdage")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/umass_statdata/chdage.dat", "chdage.hex")
            Log.info("Performing compound task ( hex[,c(\"ID\")] != 39.462276831 ) | ( hex[,c(\"AGE\")] < 25.6251597199 ) & ( ( hex[,c(\"AGE\")] < 35.3945999393 ))  on dataset <chdage>")
                     filterHex <- hex[( hex[,c("ID")] != 39.462276831 ) | ( hex[,c("AGE")] < 25.6251597199 ) & ( ( hex[,c("AGE")] < 35.3945999393 )) ,]
            Log.info("Performing compound task ( hex[,c(\"ID\")] != 20.865413136 ) | ( hex[,c(\"CHD\")] < 0.619338980336 ) & ( ( hex[,c(\"AGE\")] < 22.5728713467 ))  on dataset chdage, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("ID")] != 20.865413136 ) | ( hex[,c("CHD")] < 0.619338980336 ) & ( ( hex[,c("AGE")] < 22.5728713467 )) , c("CHD","ID","AGE")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("ID")] != 20.865413136 ) | ( hex[,c("CHD")] < 0.619338980336 ) & ( ( hex[,c("AGE")] < 22.5728713467 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data chdage", complexFilterTest_chdage_3e2ee341_5ec7_4fc2_8255_73e25cd77aa8(conn)), error = function(e) FAIL(e))
            PASS()
