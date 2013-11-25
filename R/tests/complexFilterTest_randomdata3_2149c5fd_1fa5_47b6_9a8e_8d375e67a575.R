            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_randomdata3_2149c5fd_1fa5_47b6_9a8e_8d375e67a575 <- function(conn) {
                Log.info("A munge-task R unit test on data <randomdata3> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading randomdata3")
                hex <- h2o.uploadFile(conn, "../../smalldata/randomdata3.csv", "randomdata3.hex")
            Log.info("Performing compound task !( ( hex[,c(1)] <= 969.531505394 ) | ( hex[,c(1)] == 802.406320659 ) & ( hex[,c(5)] <= -1.69963516776 ) & ( hex[,c(1)] <= 941.396883162 ) & ( ( hex[,c(1)] <= 307.099038704 )) ) on dataset <randomdata3>")
                     filterHex <- hex[!( ( hex[,c(1)] <= 969.531505394 ) | ( hex[,c(1)] == 802.406320659 ) & ( hex[,c(5)] <= -1.69963516776 ) & ( hex[,c(1)] <= 941.396883162 ) & ( ( hex[,c(1)] <= 307.099038704 )) ),]
            Log.info("Performing compound task !( ( hex[,c(4)] <= 41346.4043316 ) | ( hex[,c(3)] == 53.6962006747 ) & ( hex[,c(1)] <= 938.353741297 ) & ( hex[,c(3)] <= 49.258188299 ) & ( ( hex[,c(3)] <= 20.9377069923 )) ) on dataset randomdata3, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c(4)] <= 41346.4043316 ) | ( hex[,c(3)] == 53.6962006747 ) & ( hex[,c(1)] <= 938.353741297 ) & ( hex[,c(3)] <= 49.258188299 ) & ( ( hex[,c(3)] <= 20.9377069923 )) ), c(1,3,5,4)]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c(4)] <= 41346.4043316 ) | ( hex[,c(3)] == 53.6962006747 ) & ( hex[,c(1)] <= 938.353741297 ) & ( hex[,c(3)] <= 49.258188299 ) & ( ( hex[,c(3)] <= 20.9377069923 )) ), c(2)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data randomdata3", complexFilterTest_randomdata3_2149c5fd_1fa5_47b6_9a8e_8d375e67a575(conn)), error = function(e) FAIL(e))
            PASS()
