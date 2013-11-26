            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            complexFilterTest_claim_prediction_train_set_10000_int_47a38fb3_8e75_4ec3_8e93_4eb9fa8cc4aa <- function(conn) {
                Log.info("A munge-task R unit test on data <claim_prediction_train_set_10000_int> testing the compound functional unit <['!', '<=', '|', '==', '&', '<=', '&', '<=', '&', '<=']> ")
                Log.info("Uploading claim_prediction_train_set_10000_int")
                hex <- h2o.uploadFile(conn, "../../../smalldata/allstate/claim_prediction_train_set_10000_int.csv.gz", "claim_prediction_train_set_10000_int.hex")
            Log.info("Performing compound task !( ( hex[,c(\"Var7\")] <= -0.465044850779 ) | ( hex[,c(\"NVVar2\")] == 3.93090044997 ) & ( hex[,c(\"Var1\")] <= -0.607185951619 ) & ( hex[,c(\"Var1\")] <= 0.645258215767 ) & ( ( hex[,c(\"Vehicle\")] <= 13.4345323137 )) ) on dataset <claim_prediction_train_set_10000_int>")
                     filterHex <- hex[!( ( hex[,c("Var7")] <= -0.465044850779 ) | ( hex[,c("NVVar2")] == 3.93090044997 ) & ( hex[,c("Var1")] <= -0.607185951619 ) & ( hex[,c("Var1")] <= 0.645258215767 ) & ( ( hex[,c("Vehicle")] <= 13.4345323137 )) ),]
            Log.info("Performing compound task !( ( hex[,c(\"OrdCat\")] <= 1.03743451884 ) | ( hex[,c(\"Vehicle\")] == 10.1540096984 ) & ( hex[,c(\"NVVar2\")] <= 0.0289171567619 ) & ( hex[,c(\"NVVar3\")] <= 5.15116359051 ) & ( ( hex[,c(\"Model_Year\")] <= 1988.14337739 )) ) on dataset claim_prediction_train_set_10000_int, and also subsetting columns.")
                     filterHex <- hex[!( ( hex[,c("OrdCat")] <= 1.03743451884 ) | ( hex[,c("Vehicle")] == 10.1540096984 ) & ( hex[,c("NVVar2")] <= 0.0289171567619 ) & ( hex[,c("NVVar3")] <= 5.15116359051 ) & ( ( hex[,c("Model_Year")] <= 1988.14337739 )) ), c("Vehicle","Var6","OrdCat","Var5","Var4","NVVar1","Household_ID","Var7","Var1","NVVar4","NVVar2","NVVar3","Row_ID","Model_Year","Var8")]
                Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[!( ( hex[,c("OrdCat")] <= 1.03743451884 ) | ( hex[,c("Vehicle")] == 10.1540096984 ) & ( hex[,c("NVVar2")] <= 0.0289171567619 ) & ( hex[,c("NVVar3")] <= 5.15116359051 ) & ( ( hex[,c("Model_Year")] <= 1988.14337739 )) ), c("Cat6","Calendar_Year","Cat8","Cat7","Cat1","Cat11","Claim_Amount","Cat5","Var2","Cat9","Blind_Make","NVCat","Cat12","Var3","Cat4","Cat10","Blind_Submodel","Blind_Model","Cat2","Cat3")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("compoundFilterTest_ on data claim_prediction_train_set_10000_int", complexFilterTest_claim_prediction_train_set_10000_int_47a38fb3_8e75_4ec3_8e93_4eb9fa8cc4aa(conn)), error = function(e) FAIL(e))
            PASS()
