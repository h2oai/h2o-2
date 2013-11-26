            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_space_shuttle_damage_44e8104f_d6d4_421b_8469_788cd7620821 <- function(conn) {
                Log.info("A munge-task R unit test on data <space_shuttle_damage> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading space_shuttle_damage")
                hex <- h2o.uploadFile(conn, "../../../smalldata/space_shuttle_damage.csv", "space_shuttle_damage.hex")
            Log.info("Performing compound task !( ( hex[,c(\"temp\")] <= 59.4938305778 ) | ( hex[,c(\"temp\")] == 54.5944852955 ) & ( hex[,c(\"temp\")] <= 59.6574995198 ) & ( hex[,c(\"temp\")] <= 65.2435624523 ) & ( ( hex[,c(\"temp\")] <= 55.51802782 )) ) on dataset <space_shuttle_damage>")
                     filterHex <- hex[!( ( hex[,c("temp")] <= 59.4938305778 ) | ( hex[,c("temp")] == 54.5944852955 ) & ( hex[,c("temp")] <= 59.6574995198 ) & ( hex[,c("temp")] <= 65.2435624523 ) & ( ( hex[,c("temp")] <= 55.51802782 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"temp\")] <= 66.3745996969 ) | ( hex[,c(\"temp\")] == 74.7058143519 ) & ( hex[,c(\"temp\")] <= 75.6049909925 ) & ( hex[,c(\"temp\")] <= 78.9711670546 ) & ( ( hex[,c(\"temp\")] <= 55.2729486807 )) ) on dataset space_shuttle_damage, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("temp")] <= 66.3745996969 ) | ( hex[,c("temp")] == 74.7058143519 ) & ( hex[,c("temp")] <= 75.6049909925 ) & ( hex[,c("temp")] <= 78.9711670546 ) & ( ( hex[,c("temp")] <= 55.2729486807 )) ), c("temp")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("temp")] <= 66.3745996969 ) | ( hex[,c("temp")] == 74.7058143519 ) & ( hex[,c("temp")] <= 75.6049909925 ) & ( hex[,c("temp")] <= 78.9711670546 ) & ( ( hex[,c("temp")] <= 55.2729486807 )) ), c("damage","flight")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data space_shuttle_damage", complexFilterTest_space_shuttle_damage_44e8104f_d6d4_421b_8469_788cd7620821(conn)), error = function(e) FAIL(e))
            PASS()
