            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_cuse_73132eec_280a_43f3_b3af_f8097a6586a0 <- function(conn) {
                Log.info("A munge-task R unit test on data <cuse> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading cuse")
                hex <- h2o.uploadFile(conn, "../../smalldata/cuse.data.csv", "cuse.hex")
            Log.info("Performing compound task !( ( ( hex[,c(\"wantsmoreyes\")] != 0.833721916672 )) ) on dataset <cuse>")
                     filterHex <- hex[!( ( ( hex[,c("wantsmoreyes")] != 0.833721916672 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(\"age25to29\")] != 0.708889447618 )) ) on dataset cuse, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c("age25to29")] != 0.708889447618 )) ), c("wantsmoreyes","notUsing","agelt25","wantsmoreno","edhigh","edlow","using","age25to29","age30to39")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c("age25to29")] != 0.708889447618 )) ), c("wantsMore","age40to49","education","age")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data cuse", complexFilterTest_cuse_73132eec_280a_43f3_b3af_f8097a6586a0(conn)), error = function(e) FAIL(e))
            PASS()
