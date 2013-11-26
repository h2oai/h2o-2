            source('../Utils/h2oR.R')
            Log.info("======================== Begin Test ===========================")
            simpleFilterTest_claim_prediction_train_set_10000_cf2053ce_dedc_4a4e_9ce0_e6534aa19628 <- function(conn) {
                Log.info("A munge-task R unit test on data <claim_prediction_train_set_10000> testing the functional unit <>=> ")
                Log.info("Uploading claim_prediction_train_set_10000")
                hex <- h2o.uploadFile(conn, "../../../smalldata/allstate/claim_prediction_train_set_10000.csv.gz", "claim_prediction_train_set_10000.hex")
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"NVVar1\" using value 5.99422347801")
                     filterHex <- hex[hex[,c("NVVar1")] >= 5.99422347801,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"NVVar1" >= 5.99422347801,]
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var1\" using value 3.55671730404")
                     filterHex <- hex[hex[,c("Var1")] >= 3.55671730404,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"Var1" >= 3.55671730404,]
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var7\" using value -1.0824821929")
                     filterHex <- hex[hex[,c("Var7")] >= -1.0824821929,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"Var7" >= -1.0824821929,]
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Vehicle\" using value 10.1328574599")
                     filterHex <- hex[hex[,c("Vehicle")] >= 10.1328574599,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"Vehicle" >= 10.1328574599,]
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Calendar_Year\" using value 2005.98186751")
                     filterHex <- hex[hex[,c("Calendar_Year")] >= 2005.98186751,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"Calendar_Year" >= 2005.98186751,]
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var1\" using value 1.47225752684")
                     filterHex <- hex[hex[,c("Var1")] >= 1.47225752684,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"Var1" >= 1.47225752684,]
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"NVVar1\" using value 5.25715077132")
                     filterHex <- hex[hex[,c("NVVar1")] >= 5.25715077132,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"NVVar1" >= 5.25715077132,]
                Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var5\" using value -0.305963805361")
                     filterHex <- hex[hex[,c("Var5")] >= -0.305963805361,]
                    Log.info("Perform filtering with the '$' sign also")
                    filterHex <- hex[hex$"Var5" >= -0.305963805361,]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Household_ID\" using value 1372.71064764, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Household_ID")] >= 1372.71064764, c("Household_ID")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Household_ID")] >= 1372.71064764, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var4\" using value 2.38062460982, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Var4")] >= 2.38062460982, c("Var4")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Var4")] >= 2.38062460982, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var8\" using value 24.1834448099, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Var8")] >= 24.1834448099, c("Var8")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Var8")] >= 24.1834448099, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var2\" using value 5.87429162455, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Var2")] >= 5.87429162455, c("Var2")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Var2")] >= 5.87429162455, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Model_Year\" using value 1992.83103647, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Model_Year")] >= 1992.83103647, c("Model_Year")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Model_Year")] >= 1992.83103647, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var5\" using value 3.67061630073, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Var5")] >= 3.67061630073, c("Var5")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Var5")] >= 3.67061630073, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Household_ID\" using value 2164.79160091, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Household_ID")] >= 2164.79160091, c("Household_ID")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Household_ID")] >= 2164.79160091, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
                    Log.info("Filtering out rows by >= from dataset claim_prediction_train_set_10000 and column \"Var5\" using value 0.946141502517, and also subsetting columns.")
                     filterHex <- hex[hex[,c("Var5")] >= 0.946141502517, c("Var5")]
                    Log.info("Now do the same filter & subset, but select complement of columns.")
                     filterHex <- hex[hex[,c("Var5")] >= 0.946141502517, c("Vehicle","Cat8","NVVar1","Var6","Blind_Make","Var7","Var2","Row_ID","Model_Year","Cat2","Cat7","Cat1","Var5","Cat5","Cat11","NVVar3","NVVar2","OrdCat","Var4","Claim_Amount","Cat9","Var1","Var3","Blind_Model","Cat3","Cat6","Calendar_Year","Household_ID","NVCat","Cat4","NVVar4","Var8","Cat10","Blind_Submodel","Cat12")]
            }
            conn = new("H2OClient", ip=myIP, port=myPort)
            tryCatch(test_that("simpleFilterTest_ on data claim_prediction_train_set_10000", simpleFilterTest_claim_prediction_train_set_10000_cf2053ce_dedc_4a4e_9ce0_e6534aa19628(conn)), error = function(e) FAIL(e))
            PASS()
