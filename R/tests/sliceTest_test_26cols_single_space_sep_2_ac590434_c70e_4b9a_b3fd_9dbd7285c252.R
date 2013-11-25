            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            sliceTest_test_26cols_single_space_sep_2_ac590434_c70e_4b9a_b3fd_9dbd7285c252 <- function(conn) {
                Log.info("A munge-task R unit test on data <test_26cols_single_space_sep_2> testing the functional unit <[> ")
                Log.info("Uploading test_26cols_single_space_sep_2")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/test_26cols_single_space_sep_2.csv", "test_26cols_single_space_sep_2.hex")
                Log.info("Performing a column slice of test_26cols_single_space_sep_2 using these columns: c(\"A\",\"B\")")
                slicedHex <- hex[,c("A","B")]
                    Log.info("Performing a row & column slice of test_26cols_single_space_sep_2 using these rows & columns: c(\"K\",\"H\",\"T\",\"U\",\"N\",\"O\",\"Y\") & c(1)")
                slicedHex <- hex[c(1),c("K","H","T","U","N","O","Y")]
                    Log.info("Performing a 1-by-1 column slice of test_26cols_single_space_sep_2 using these columns: ")
                    Log.info("Performing a 1-by-1 row slice of test_26cols_single_space_sep_2 using these rows: ")
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("sliceTest_ on data test_26cols_single_space_sep_2", sliceTest_test_26cols_single_space_sep_2_ac590434_c70e_4b9a_b3fd_9dbd7285c252(conn)), error = function(e) FAIL(e))
            PASS()
