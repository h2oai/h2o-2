            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_sumsigmoids_test_3379d791_372a_41f1_85b3_2d70e80a0c1c <- function(conn) {
                Log.info("A munge-task R unit test on data <sumsigmoids_test> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading sumsigmoids_test")
                hex <- h2o.uploadFile(conn, "../../../smalldata/neural/sumsigmoids_test.csv", "sumsigmoids_test.hex")
            Log.info("Performing compound task !( ( hex[,c(\"X.2\")] <= 1.2959283047 ) | ( hex[,c(\"Y\")] == -0.197639617336 ) & ( hex[,c(\"X.1\")] <= -0.300170765328 ) & ( hex[,c(\"X.1\")] <= -1.22169674577 ) & ( ( hex[,c(\"Y\")] <= 1.45507811475 )) ) on dataset <sumsigmoids_test>")
                     filterHex <- hex[!( ( hex[,c("X.2")] <= 1.2959283047 ) | ( hex[,c("Y")] == -0.197639617336 ) & ( hex[,c("X.1")] <= -0.300170765328 ) & ( hex[,c("X.1")] <= -1.22169674577 ) & ( ( hex[,c("Y")] <= 1.45507811475 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"X.2\")] <= 3.45889362047 ) | ( hex[,c(\"X.2\")] == -3.02844055889 ) & ( hex[,c(\"X.2\")] <= -1.9716979757 ) & ( hex[,c(\"X.2\")] <= 0.0207968640788 ) & ( ( hex[,c(\"Y\")] <= 0.601033531268 )) ) on dataset sumsigmoids_test, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("X.2")] <= 3.45889362047 ) | ( hex[,c("X.2")] == -3.02844055889 ) & ( hex[,c("X.2")] <= -1.9716979757 ) & ( hex[,c("X.2")] <= 0.0207968640788 ) & ( ( hex[,c("Y")] <= 0.601033531268 )) ), c("X.1","Y","X.2")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("X.2")] <= 3.45889362047 ) | ( hex[,c("X.2")] == -3.02844055889 ) & ( hex[,c("X.2")] <= -1.9716979757 ) & ( hex[,c("X.2")] <= 0.0207968640788 ) & ( ( hex[,c("Y")] <= 0.601033531268 )) ), c(1)]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data sumsigmoids_test", complexFilterTest_sumsigmoids_test_3379d791_372a_41f1_85b3_2d70e80a0c1c(conn)), error = function(e) FAIL(e))
            PASS()
