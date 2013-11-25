            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_AirlinesTest_3f4aff28_4966_4c31_b007_6d012f54f802 <- function(conn) {
                Log.info("A munge-task R unit test on data <AirlinesTest> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading AirlinesTest")
                hex <- h2o.uploadFile(conn, "../../../smalldata/airlines/AirlinesTest.csv.zip", "AirlinesTest.hex")
            Log.info("Performing compound task !( ( ( hex[,c(\"Distance\")] != 2899.36699946 )) ) on dataset <AirlinesTest>")
                     filterHex <- hex[!( ( ( hex[,c("Distance")] != 2899.36699946 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(\"DepTime\")] != 162.556113517 )) ) on dataset AirlinesTest, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c("DepTime")] != 162.556113517 )) ), c("DepTime","Distance","IsDepDelayed_REC","ArrTime")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c("DepTime")] != 162.556113517 )) ), c("Origin","fDayofMonth","fMonth","fDayOfWeek","IsDepDelayed","fYear","Dest","UniqueCarrier")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data AirlinesTest", complexFilterTest_AirlinesTest_3f4aff28_4966_4c31_b007_6d012f54f802(conn)), error = function(e) FAIL(e))
            PASS()
