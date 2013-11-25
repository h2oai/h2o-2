            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_test_manycol_tree_5f34bcc1_f593_4147_b346_802993f00f87 <- function(conn) {
                Log.info("A munge-task R unit test on data <test_manycol_tree> testing the functional unit <>=> ")
                Log.info("Uploading test_manycol_tree")
                hex <- h2o.uploadFile(conn, "../../smalldata/test/test_manycol_tree.csv", "test_manycol_tree.hex")
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"B\" using value 1.97800824659")
                     filterHex <- hex[hex[,c("B")] >= 1.97800824659,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"B" >= 1.97800824659,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"D\" using value 2.80597255661")
                     filterHex <- hex[hex[,c("D")] >= 2.80597255661,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"D" >= 2.80597255661,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"B\" using value 2.30552843847")
                     filterHex <- hex[hex[,c("B")] >= 2.30552843847,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"B" >= 2.30552843847,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"X\" using value 2.2632683074")
                     filterHex <- hex[hex[,c("X")] >= 2.2632683074,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"X" >= 2.2632683074,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"D\" using value 1.22871311948")
                     filterHex <- hex[hex[,c("D")] >= 1.22871311948,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"D" >= 1.22871311948,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"X\" using value 1.70878381659")
                     filterHex <- hex[hex[,c("X")] >= 1.70878381659,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"X" >= 1.70878381659,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"X\" using value 1.46637095244")
                     filterHex <- hex[hex[,c("X")] >= 1.46637095244,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"X" >= 1.46637095244,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"A\" using value 1.89237896239")
                     filterHex <- hex[hex[,c("A")] >= 1.89237896239,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"A" >= 1.89237896239,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"A\" using value 1.59428313584")
                     filterHex <- hex[hex[,c("A")] >= 1.59428313584,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"A" >= 1.59428313584,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"A\" using value 2.15105109857")
                     filterHex <- hex[hex[,c("A")] >= 2.15105109857,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"A" >= 2.15105109857,]
                Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"B\" using value 2.98410581401")
                     filterHex <- hex[hex[,c("B")] >= 2.98410581401,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"B" >= 2.98410581401,]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"A\" using value 2.88886868266, and also subsetting columns.")
                     filterHex <- hex[hex[,c("A")] >= 2.88886868266, c("A")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("A")] >= 2.88886868266, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"X\" using value 1.8140281979, and also subsetting columns.")
                     filterHex <- hex[hex[,c("X")] >= 1.8140281979, c("X")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("X")] >= 1.8140281979, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"B\" using value 1.47724696533, and also subsetting columns.")
                     filterHex <- hex[hex[,c("B")] >= 1.47724696533, c("B")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("B")] >= 1.47724696533, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"X\" using value 2.11117438653, and also subsetting columns.")
                     filterHex <- hex[hex[,c("X")] >= 2.11117438653, c("X")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("X")] >= 2.11117438653, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"A\" using value 1.53722584958, and also subsetting columns.")
                     filterHex <- hex[hex[,c("A")] >= 1.53722584958, c("A")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("A")] >= 1.53722584958, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"C\" using value 1.12764743494, and also subsetting columns.")
                     filterHex <- hex[hex[,c("C")] >= 1.12764743494, c("C")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("C")] >= 1.12764743494, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"B\" using value 2.82883238525, and also subsetting columns.")
                     filterHex <- hex[hex[,c("B")] >= 2.82883238525, c("B")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("B")] >= 2.82883238525, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"A\" using value 2.17300109586, and also subsetting columns.")
                     filterHex <- hex[hex[,c("A")] >= 2.17300109586, c("A")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("A")] >= 2.17300109586, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"B\" using value 2.71126168228, and also subsetting columns.")
                     filterHex <- hex[hex[,c("B")] >= 2.71126168228, c("B")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("B")] >= 2.71126168228, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"B\" using value 2.8613592604, and also subsetting columns.")
                     filterHex <- hex[hex[,c("B")] >= 2.8613592604, c("B")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("B")] >= 2.8613592604, c("response","D","B","C","A","X")]
                    Log.info("Filtering out rows by >= from dataset test_manycol_tree and column \"A\" using value 1.37919430622, and also subsetting columns.")
                     filterHex <- hex[hex[,c("A")] >= 1.37919430622, c("A")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("A")] >= 1.37919430622, c("response","D","B","C","A","X")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data test_manycol_tree", simpleFilterTest_test_manycol_tree_5f34bcc1_f593_4147_b346_802993f00f87(conn)), error = function(e) FAIL(e))
            PASS()
