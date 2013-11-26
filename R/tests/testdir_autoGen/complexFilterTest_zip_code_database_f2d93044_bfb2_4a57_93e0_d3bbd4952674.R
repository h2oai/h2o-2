            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_zip_code_database_f2d93044_bfb2_4a57_93e0_d3bbd4952674 <- function(conn) {
                Log.info("A munge-task R unit test on data <zip_code_database> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading zip_code_database")
                hex <- h2o.uploadFile(conn, "../../../smalldata/zip_code/zip_code_database.csv.gz", "zip_code_database.hex")
            Log.info("Performing compound task !( ( hex[,c(\"zip\")] <= 81048.3030954 ) | ( hex[,c(\"unacceptable_cities\")] == 88.0 ) & ( hex[,c(\"unacceptable_cities\")] <= 88.0 ) & ( hex[,c(\"area_codes\")] <= 468.262701938 ) & ( ( hex[,c(\"area_codes\")] <= 485.917050832 )) ) on dataset <zip_code_database>")
                     filterHex <- hex[!( ( hex[,c("zip")] <= 81048.3030954 ) | ( hex[,c("unacceptable_cities")] == 88.0 ) & ( hex[,c("unacceptable_cities")] <= 88.0 ) & ( hex[,c("area_codes")] <= 468.262701938 ) & ( ( hex[,c("area_codes")] <= 485.917050832 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"area_codes\")] <= 833.485734961 ) | ( hex[,c(\"estimated_population\")] == 65717.3148094 ) & ( hex[,c(\"longitude\")] <= -72.3239823346 ) & ( hex[,c(\"decommissioned\")] <= 0.251853930854 ) & ( ( hex[,c(\"longitude\")] <= -130.620272924 )) ) on dataset zip_code_database, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("area_codes")] <= 833.485734961 ) | ( hex[,c("estimated_population")] == 65717.3148094 ) & ( hex[,c("longitude")] <= -72.3239823346 ) & ( hex[,c("decommissioned")] <= 0.251853930854 ) & ( ( hex[,c("longitude")] <= -130.620272924 )) ), c("area_codes","unacceptable_cities","decommissioned","zip","longitude","estimated_population","latitude")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("area_codes")] <= 833.485734961 ) | ( hex[,c("estimated_population")] == 65717.3148094 ) & ( hex[,c("longitude")] <= -72.3239823346 ) & ( hex[,c("decommissioned")] <= 0.251853930854 ) & ( ( hex[,c("longitude")] <= -130.620272924 )) ), c("timezone","county","world_region","state","country","type","primary_city","notes","acceptable_cities")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data zip_code_database", complexFilterTest_zip_code_database_f2d93044_bfb2_4a57_93e0_d3bbd4952674(conn)), error = function(e) FAIL(e))
            PASS()
