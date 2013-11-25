            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_pros_3fecbfaf_30b8_4541_a211_05c7fa7a39dd <- function(conn) {
                Log.info("A munge-task R unit test on data <pros> testing the functional unit <>=> ")
                Log.info("Uploading pros")
                hex <- h2o.uploadFile(conn, "../../smalldata/logreg/pros.xls", "pros.hex")
                Log.info("Filtering out rows by >= from dataset pros and column \"ID\" using value 335.4593244")
                     filterHex <- hex[hex[,c("ID")] >= 335.4593244,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"ID" >= 335.4593244,]
                Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 106.006372297")
                     filterHex <- hex[hex[,c("PSA")] >= 106.006372297,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"PSA" >= 106.006372297,]
                Log.info("Filtering out rows by >= from dataset pros and column \"CAPSULE\" using value 0.833049219416")
                     filterHex <- hex[hex[,c("CAPSULE")] >= 0.833049219416,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"CAPSULE" >= 0.833049219416,]
                Log.info("Filtering out rows by >= from dataset pros and column \"RACE\" using value 1.61778103526")
                     filterHex <- hex[hex[,c("RACE")] >= 1.61778103526,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"RACE" >= 1.61778103526,]
                Log.info("Filtering out rows by >= from dataset pros and column \"VOL\" using value 7.00024126569")
                     filterHex <- hex[hex[,c("VOL")] >= 7.00024126569,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"VOL" >= 7.00024126569,]
                Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 45.2898477771")
                     filterHex <- hex[hex[,c("PSA")] >= 45.2898477771,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"PSA" >= 45.2898477771,]
                Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 81.3453775434")
                     filterHex <- hex[hex[,c("PSA")] >= 81.3453775434,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"PSA" >= 81.3453775434,]
                Log.info("Filtering out rows by >= from dataset pros and column \"GLEASON\" using value 4.31664721588")
                     filterHex <- hex[hex[,c("GLEASON")] >= 4.31664721588,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"GLEASON" >= 4.31664721588,]
                Log.info("Filtering out rows by >= from dataset pros and column \"ID\" using value 132.71653631")
                     filterHex <- hex[hex[,c("ID")] >= 132.71653631,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"ID" >= 132.71653631,]
                Log.info("Filtering out rows by >= from dataset pros and column \"DPROS\" using value 2.68578841864")
                     filterHex <- hex[hex[,c("DPROS")] >= 2.68578841864,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"DPROS" >= 2.68578841864,]
                Log.info("Filtering out rows by >= from dataset pros and column \"GLEASON\" using value 4.11572512993")
                     filterHex <- hex[hex[,c("GLEASON")] >= 4.11572512993,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"GLEASON" >= 4.11572512993,]
                Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 63.8734471331")
                     filterHex <- hex[hex[,c("PSA")] >= 63.8734471331,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"PSA" >= 63.8734471331,]
                Log.info("Filtering out rows by >= from dataset pros and column \"DCAPS\" using value 1.97595074815")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.97595074815,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"DCAPS" >= 1.97595074815,]
                Log.info("Filtering out rows by >= from dataset pros and column \"CAPSULE\" using value 0.168759142729")
                     filterHex <- hex[hex[,c("CAPSULE")] >= 0.168759142729,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"CAPSULE" >= 0.168759142729,]
                Log.info("Filtering out rows by >= from dataset pros and column \"DCAPS\" using value 1.54043425274")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.54043425274,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"DCAPS" >= 1.54043425274,]
                    Log.info("Filtering out rows by >= from dataset pros and column \"DCAPS\" using value 1.88014417191, and also subsetting columns.")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.88014417191, c("DCAPS")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.88014417191, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 71.8688801532, and also subsetting columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 71.8688801532, c("PSA")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 71.8688801532, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"AGE\" using value 51.9756476607, and also subsetting columns.")
                     filterHex <- hex[hex[,c("AGE")] >= 51.9756476607, c("AGE")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("AGE")] >= 51.9756476607, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"VOL\" using value 87.8590262778, and also subsetting columns.")
                     filterHex <- hex[hex[,c("VOL")] >= 87.8590262778, c("VOL")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("VOL")] >= 87.8590262778, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"VOL\" using value 96.4485226157, and also subsetting columns.")
                     filterHex <- hex[hex[,c("VOL")] >= 96.4485226157, c("VOL")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("VOL")] >= 96.4485226157, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"RACE\" using value 1.78874969154, and also subsetting columns.")
                     filterHex <- hex[hex[,c("RACE")] >= 1.78874969154, c("RACE")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("RACE")] >= 1.78874969154, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"DPROS\" using value 3.99467465469, and also subsetting columns.")
                     filterHex <- hex[hex[,c("DPROS")] >= 3.99467465469, c("DPROS")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("DPROS")] >= 3.99467465469, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"DCAPS\" using value 1.33134925109, and also subsetting columns.")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.33134925109, c("DCAPS")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.33134925109, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"VOL\" using value 60.799180588, and also subsetting columns.")
                     filterHex <- hex[hex[,c("VOL")] >= 60.799180588, c("VOL")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("VOL")] >= 60.799180588, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 39.4167773655, and also subsetting columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 39.4167773655, c("PSA")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 39.4167773655, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"DPROS\" using value 2.63006237429, and also subsetting columns.")
                     filterHex <- hex[hex[,c("DPROS")] >= 2.63006237429, c("DPROS")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("DPROS")] >= 2.63006237429, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"DCAPS\" using value 1.05159094125, and also subsetting columns.")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.05159094125, c("DCAPS")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("DCAPS")] >= 1.05159094125, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 103.708090088, and also subsetting columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 103.708090088, c("PSA")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 103.708090088, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"PSA\" using value 24.674107548, and also subsetting columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 24.674107548, c("PSA")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("PSA")] >= 24.674107548, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
                    Log.info("Filtering out rows by >= from dataset pros and column \"GLEASON\" using value 0.202273110654, and also subsetting columns.")
                     filterHex <- hex[hex[,c("GLEASON")] >= 0.202273110654, c("GLEASON")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("GLEASON")] >= 0.202273110654, c("GLEASON","DPROS","PSA","DCAPS","VOL","CAPSULE","RACE","ID","AGE")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data pros", simpleFilterTest_pros_3fecbfaf_30b8_4541_a211_05c7fa7a39dd(conn)), error = function(e) FAIL(e))
            PASS()
