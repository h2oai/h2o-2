            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_prostate_train_d79bd9e1_7392_4dbe_8d92_a8e20e1f9de7 <- function(conn) {
                Log.info("A munge-task R unit test on data <prostate_train> testing the compound functional unit <['', '!=', '|', '==', '&', '!|', '!', '>']> ")
                Log.info("Uploading prostate_train")
                hex <- h2o.uploadFile(conn, "../../../smalldata/logreg/prostate_train.csv", "prostate_train.hex")
            Log.info("Performing compound task ( hex[,c(\"VOL\")] != 1.86147116914 ) | ( hex[,c(\"GLEASON\")] == -3.65175868348 ) & ( hex[,c(\"AGE\")] !| -1.91367748891 ) ! ( hex[,c(\"DCAPS\")] > 2.64513382128 ) on dataset <prostate_train>")
                     filterHex <- hex[( hex[,c("VOL")] != 1.86147116914 ) | ( hex[,c("GLEASON")] == -3.65175868348 ) & ( hex[,c("AGE")] !| -1.91367748891 ) ! ( hex[,c("DCAPS")] > 2.64513382128 ),]
            Log.info("Performing compound task ( hex[,c(\"AGE\")] != 0.522511648861 ) | ( hex[,c(\"VOL\")] == 2.60182388846 ) & ( hex[,c(\"VOL\")] !| 3.99036403618 ) ! ( hex[,c(\"PSA\")] > 2.03634833457 ) on dataset prostate_train, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("AGE")] != 0.522511648861 ) | ( hex[,c("VOL")] == 2.60182388846 ) & ( hex[,c("VOL")] !| 3.99036403618 ) ! ( hex[,c("PSA")] > 2.03634833457 ), c("PSA","DCAPS","VOL","AGE","DPROS")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("AGE")] != 0.522511648861 ) | ( hex[,c("VOL")] == 2.60182388846 ) & ( hex[,c("VOL")] !| 3.99036403618 ) ! ( hex[,c("PSA")] > 2.03634833457 ), c("CAPSULE","GLEASON","RACE")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data prostate_train", complexFilterTest_prostate_train_d79bd9e1_7392_4dbe_8d92_a8e20e1f9de7(conn)), error = function(e) FAIL(e))
            PASS()
