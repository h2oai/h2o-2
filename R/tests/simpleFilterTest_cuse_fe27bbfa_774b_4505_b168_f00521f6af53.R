            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_cuse_fe27bbfa_774b_4505_b168_f00521f6af53 <- function(conn) {
                Log.info("A munge-task R unit test on data <cuse> testing the functional unit <>=> ")
                Log.info("Uploading cuse")
                hex <- h2o.uploadFile(conn, "../../smalldata/logreg/princeton/cuse.dat", "cuse.hex")
                Log.info("Filtering out rows by >= from dataset cuse and column \"using\" using value 4.7485605517")
                     filterHex <- hex[hex[,c("using")] >= 4.7485605517,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"using" >= 4.7485605517,]
                Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 195.644150581")
                     filterHex <- hex[hex[,c("notUsing")] >= 195.644150581,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"notUsing" >= 195.644150581,]
                Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 31.0959143655")
                     filterHex <- hex[hex[,c("notUsing")] >= 31.0959143655,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"notUsing" >= 31.0959143655,]
                Log.info("Filtering out rows by >= from dataset cuse and column \"using\" using value 10.5981846968")
                     filterHex <- hex[hex[,c("using")] >= 10.5981846968,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"using" >= 10.5981846968,]
                Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 93.197791016")
                     filterHex <- hex[hex[,c("notUsing")] >= 93.197791016,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"notUsing" >= 93.197791016,]
                Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 83.4152565026")
                     filterHex <- hex[hex[,c("notUsing")] >= 83.4152565026,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"notUsing" >= 83.4152565026,]
                Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 148.577509364")
                     filterHex <- hex[hex[,c("notUsing")] >= 148.577509364,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"notUsing" >= 148.577509364,]
                    Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 101.103192198, and also subsetting columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 101.103192198, c("notUsing")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 101.103192198, c("wantsMore","using","education","age","notUsing")]
                    Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 142.644296008, and also subsetting columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 142.644296008, c("notUsing")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 142.644296008, c("wantsMore","using","education","age","notUsing")]
                    Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 55.2762526509, and also subsetting columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 55.2762526509, c("notUsing")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 55.2762526509, c("wantsMore","using","education","age","notUsing")]
                    Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 24.6266345421, and also subsetting columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 24.6266345421, c("notUsing")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 24.6266345421, c("wantsMore","using","education","age","notUsing")]
                    Log.info("Filtering out rows by >= from dataset cuse and column \"using\" using value 68.938439554, and also subsetting columns.")
                     filterHex <- hex[hex[,c("using")] >= 68.938439554, c("using")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("using")] >= 68.938439554, c("wantsMore","using","education","age","notUsing")]
                    Log.info("Filtering out rows by >= from dataset cuse and column \"notUsing\" using value 56.4034590768, and also subsetting columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 56.4034590768, c("notUsing")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("notUsing")] >= 56.4034590768, c("wantsMore","using","education","age","notUsing")]
                    Log.info("Filtering out rows by >= from dataset cuse and column \"using\" using value 58.8266697085, and also subsetting columns.")
                     filterHex <- hex[hex[,c("using")] >= 58.8266697085, c("using")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("using")] >= 58.8266697085, c("wantsMore","using","education","age","notUsing")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data cuse", simpleFilterTest_cuse_fe27bbfa_774b_4505_b168_f00521f6af53(conn)), error = function(e) FAIL(e))
            PASS()
