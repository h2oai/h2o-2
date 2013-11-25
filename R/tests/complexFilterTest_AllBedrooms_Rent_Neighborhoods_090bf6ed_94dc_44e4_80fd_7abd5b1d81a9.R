            source('./Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_AllBedrooms_Rent_Neighborhoods_090bf6ed_94dc_44e4_80fd_7abd5b1d81a9 <- function(conn) {
                Log.info("A munge-task R unit test on data <AllBedrooms_Rent_Neighborhoods> testing the compound functional unit <['!', '!=']> ")
                Log.info("Uploading AllBedrooms_Rent_Neighborhoods")
                hex <- h2o.uploadFile(conn, "../../smalldata/categoricals/AllBedrooms_Rent_Neighborhoods.csv.gz", "AllBedrooms_Rent_Neighborhoods.hex")
            Log.info("Performing compound task !( ( ( hex[,c(\"count49\")] != 10598.5132528 )) ) on dataset <AllBedrooms_Rent_Neighborhoods>")
                     filterHex <- hex[!( ( ( hex[,c("count49")] != 10598.5132528 )) ),]
            Log.info("Performing compound task !( ( ( hex[,c(\"count39\")] != 47553.8829688 )) ) on dataset AllBedrooms_Rent_Neighborhoods, and also subsetting columns.")
                     filterHex <- hex[!( ( ( hex[,c("count39")] != 47553.8829688 )) ), c("metro","count5","count18","count54","count42","count76","count15","count26","count1","place","count30","count57","count91","count79","count83","count87","count85","count65","count92","count39","state","count19","count88","count51","count78","count23")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( ( hex[,c("count39")] != 47553.8829688 )) ), c("mcd","count81","count7","count93","count68","count40","count46","count44","count72","count3","count50","count99","count52","count59","Rent_Type","count20","count49","count36","count24","count45","count32","count8","county","count74","areaname","count67","count97","count38","count13","count95","count34","count17","count98","count11","count22","count6","count70","count4","count28","count90","count63","count80","count29","medrent","count53","count58","count55","count62","count61","count47","count77","count89","count43","count75","count41","count2","count60","count31","count14","count37","count16","count21","count33","sumlevel","count69","count25","count27","count9","count71","count12","count84","count82","count66","count56","count86","count73","count48","count94","count64","count96","count35","count10")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data AllBedrooms_Rent_Neighborhoods", complexFilterTest_AllBedrooms_Rent_Neighborhoods_090bf6ed_94dc_44e4_80fd_7abd5b1d81a9(conn)), error = function(e) FAIL(e))
            PASS()
