            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_AllBedrooms_Rent_Neighborhoods_9ce72e2e_ebf3_4b41_b59f_1728d3172614 <- function(conn) {
                Log.info("A munge-task R unit test on data <AllBedrooms_Rent_Neighborhoods> testing the compound functional unit <['', '!=', '|', '<', '&', '<']> ")
                Log.info("Uploading AllBedrooms_Rent_Neighborhoods")
                hex <- h2o.uploadFile(conn, "../../../smalldata/categoricals/AllBedrooms_Rent_Neighborhoods.csv.gz", "AllBedrooms_Rent_Neighborhoods.hex")
            Log.info("Performing compound task ( hex[,c(\"count14\")] != 229593.996541 ) | ( hex[,c(\"count73\")] < 3142.0197764 ) & ( ( hex[,c(\"count56\")] < 17922.6924661 ))  on dataset <AllBedrooms_Rent_Neighborhoods>")
                     filterHex <- hex[( hex[,c("count14")] != 229593.996541 ) | ( hex[,c("count73")] < 3142.0197764 ) & ( ( hex[,c("count56")] < 17922.6924661 )) ,]
            Log.info("Performing compound task ( hex[,c(\"count80\")] != 1375.57734805 ) | ( hex[,c(\"count97\")] < 971.326360191 ) & ( ( hex[,c(\"count20\")] < 497951.387286 ))  on dataset AllBedrooms_Rent_Neighborhoods, and also subsetting columns.")
                     filterHex <- hex[( hex[,c("count80")] != 1375.57734805 ) | ( hex[,c("count97")] < 971.326360191 ) & ( ( hex[,c("count20")] < 497951.387286 )) , c("count81","count7","count46","count72","count76","count74","count59","count26","count20","count63","count8","count52","count97","count91","count34","count17","count87","count22","count65","count15","count39","count80","medrent","count75","count47","count51","count69","count60","count58","count16","sumlevel","count25","count27","count78","count62")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[( hex[,c("count80")] != 1375.57734805 ) | ( hex[,c("count97")] < 971.326360191 ) & ( ( hex[,c("count20")] < 497951.387286 )) , c("mcd","count57","count1","count5","count54","count28","count93","count42","count40","count44","count18","count3","count50","count99","count89","count68","Rent_Type","metro","count36","count24","count45","count32","place","count30","county","areaname","count67","count38","count13","count83","count95","count98","count11","count85","count6","count79","count4","count90","state","count29","count55","count53","count61","count19","count88","count77","count43","count41","count73","count2","count49","count31","count14","count37","count21","count33","count56","count92","count9","count71","count12","count84","count82","count66","count70","count86","count23","count48","count94","count64","count96","count35","count10")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data AllBedrooms_Rent_Neighborhoods", complexFilterTest_AllBedrooms_Rent_Neighborhoods_9ce72e2e_ebf3_4b41_b59f_1728d3172614(conn)), error = function(e) FAIL(e))
            PASS()
