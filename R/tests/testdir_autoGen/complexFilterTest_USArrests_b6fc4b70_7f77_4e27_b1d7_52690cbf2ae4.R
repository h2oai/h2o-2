            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_USArrests_b6fc4b70_7f77_4e27_b1d7_52690cbf2ae4 <- function(conn) {
                Log.info("A munge-task R unit test on data <USArrests> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading USArrests")
                hex <- h2o.uploadFile(conn, "../../../smalldata/pca_test/USArrests.csv", "USArrests.hex")
            Log.info("Performing compound task ( hex[,c(\"Rape\")] != 29.0433841965 ) | ( hex[,c(\"Assault\")] < 305.53057837 ) & ( ( hex[,c(\"Assault\")] < 80.9612165181 ))  on dataset <USArrests>")
                     filterHex <- hex[( hex[,c("Rape")] != 29.0433841965 ) | ( hex[,c("Assault")] < 305.53057837 ) & ( ( hex[,c("Assault")] < 80.9612165181 )) ,]
            Log.info("Performing compound task ( hex[,c(\"Murder\")] != 13.280322888 ) | ( hex[,c(\"Assault\")] < 158.450499626 ) & ( ( hex[,c(\"Rape\")] < 7.53939156756 ))  on dataset USArrests, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("Murder")] != 13.280322888 ) | ( hex[,c("Assault")] < 158.450499626 ) & ( ( hex[,c("Rape")] < 7.53939156756 )) , c("UrbanPop","Assault","Rape","Murder")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("Murder")] != 13.280322888 ) | ( hex[,c("Assault")] < 158.450499626 ) & ( ( hex[,c("Rape")] < 7.53939156756 )) , c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data USArrests", complexFilterTest_USArrests_b6fc4b70_7f77_4e27_b1d7_52690cbf2ae4(conn)), error = function(e) FAIL(e))
            PASS()
