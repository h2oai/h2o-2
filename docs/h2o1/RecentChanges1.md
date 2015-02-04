#Recent Changes

##H2O-1

###Noether Release (2.8.4.1) - 1/30/15

####Enhancements 

These changes are improvements to existing features (which includes changed default values):

#####UI

- Java POJO scoring implemented for unsupervised models (especially DL Autoencoder) [(PUB-1118)](https://0xdata.atlassian.net/browse/PUB-1118)
- Added perfbar to Cluster Status [(PUB-296)](https://0xdata.atlassian.net/browse/PUB-296)
- Raised default stack trace depth for Profiler to 10 [(github)](https://github.com/h2oai/h2o/commit/08604c6d9663cbf0c9d955fff137faf68048eb7d)
- Reduced linewidth to <90 chars. [(github)](https://github.com/h2oai/h2o/commit/b91aad3bcb4db4428d5e5600a3d17040cf7167fcO)
- Added warning and listed remedies if the input layer is bigger than 100k neurons [(github)](https://github.com/h2oai/h2o/commit/ae78448b9dabb4dd10ad099cdae7abd5d73c65dd)
- Added license info [(github)](https://github.com/h2oai/h2o/commit/8f51b83f671fa63ef4e2118836a8f20648c0b282)

#####API
- Added extra options to `CreateFrame` to make (potentially sparse) binary columns [(github)](https://github.com/h2oai/h2o/commit/7e0d6509fde1733fcfe17f92f5c65abc8597fa8a)
- Ported changes to `createFrame` to R [(github)](https://github.com/h2oai/h2o/commit/f5b2a4d60076dff61414e8dbc246780ade1132ad)
- Added chunk summary after `CreateFrame` [(github)](https://github.com/h2oai/h2o/commit/6e83e39ca6797e03db97ce925afe6ad71bcba56f)
- Added `makeModel` call to R/H2O to allow for hand-made glm models with efficient scoring [(github)](https://github.com/h2oai/h2o/commit/4603531579f9e890df1d0fdaf5b57de3719c5e8e)
- Raised default value of `-data_max_factor_levels` from 65k to 1M [(github)](https://github.com/h2oai/h2o/commit/452392e17a08489103b2063955761702e637d905)
- Updated `h2o.createFrame` with optional response column [(github)](https://github.com/h2oai/h2o/commit/372bb4a94a1b404328c4c50ad780a480bf49efa8)
- Added `many_cols` and `chunk_bytes` to h2o.init [(github)](https://github.com/h2oai/h2o/commit/5edb09f8e74544847f063f17a2ec654803aabada)
- Added `data_max_factor_levels` to h2o.init() in R [(github)](https://github.com/h2oai/h2o/commit/f7a7defc3411d5e16cd8d9ab6690119c5b94a363)
- Added `-many_cols` (same as `-chunk_bits` 24), renamed `-chunk_bits` to `-chunk_bytes` [(github)](https://github.com/h2oai/h2o/commit/524bef3102e43a2d09ec337f2c6ebf5e75ebab38)
- Added glm `disable_line_search` flag to R [(github)](https://github.com/h2oai/h2o/commit/e1211d487f15885f76c1bf9a56c1f37180f8a0e7)
- Added (hidden) explicit `disable_line_search` flag to allow user to disable line search [(github)](https://github.com/h2oai/h2o/commit/7b346d76f2b474656d2c72bf7c37a8d778acbdac)
- Added `-data_max_factor_levels` and `-chunk_bits` to Hadoop command line [(github)](https://github.com/h2oai/h2o/commit/e0cb1a8fdfd251d8c27a14dfc295ac24af1b7098)
- Made chunk size an argument. Default is `-chunk_bits` 24 -> 16MB [(github)](https://github.com/h2oai/h2o/commit/014dda94e2e055dff42e99f9e03897da08d51080)
- Updated parameters for py API for createFrame [(github)](https://github.com/h2oai/h2o/commit/53ab3196983ffae0df0f73679c0bdbe53052c357)
- Updated create frame arguments [(github)](https://github.com/h2oai/h2o/commit/ddf0da8abddeba5a1da5d71a1490d0deb465f8b8)
- Added `runifSplit` to Frame [(github)](https://github.com/h2oai/h2o/commit/8a3b7d5cfa02ed32ea454293b79677bb66cda8fa)
- Added support for `holdout_fraction` to SRF/DRF/GBM/DL in R [(github)](https://github.com/h2oai/h2o/commit/47a491070bd111c1904eca5a0f3dbf00b8f6e890)
- Backported rbind v2 from h2o-dev [(github)](https://github.com/h2oai/h2o/commit/f0ff6f027e4aa9e3faebcd5f192bd8781d47c5f1)
- Added `frozenType()` override to speed up histogram deserialization [(github)](https://github.com/h2oai/h2o/commit/b19d95774d008990441d27bcbc88df4be4eb0e79)

#####Algorithms

- Automatically call SplitFrame from the ModelBuilder page [(PUB-1103)](https://0xdata.atlassian.net/browse/PUB-1103)
- PUB-1103: Added option to automatically call SplitFrame from model builder age if holdout_fraction is > 0 [(github)](https://github.com/h2oai/h2o/commit/e054707a2c627944d8441a6f5562bf2ec216a459)
- Increased epochs to 30 and increased tolerance to .4 [(github)](https://github.com/h2oai/h2o/commit/8ceb7acf88fbe059f5f6034b297aeb74de9f6454)
- Enabled variable importance by default [(github)](https://github.com/h2oai/h2o/commit/de5457971a956318c52e6c15b06b95932e3d4d30)
- Changed GLM printout to avoid errors with modified glm models [(github)](https://github.com/h2oai/h2o/commit/13d4c73887976e4ed2fdc25e249a86e4e9c63f11)
- Added survfit class designation to h2o.coxph survfit output [(github)](https://github.com/h2oai/h2o/commit/b504526916179aa4265f87767a548d7978ea7ccb)
- Changed predict method for CoxPH to return the risk as opposed to the cumulative hazard and associated standard error [(github)](https://github.com/h2oai/h2o/commit/fa8af63a02b5ba9a0bbe1df5f1e1e1cea955fcd2)
- Added N-fold CV, use GBM for MNIST [(github)](https://github.com/h2oai/h2o/commit/60a1957c5f4a0734a1d179b93c77bf5de323fde0)
- Added grid parallelism parameter to h2o.gbm [(github)](https://github.com/h2oai/h2o/commit/6e9a3577351e606a6da5543580ddca7ff864622c)
- Updated HDFS deletion command, and run GBM before DRF [(github)](https://github.com/h2oai/h2o/commit/28c499084019bc7566826a2b933c3720091b12df)
- Added script for running GLM/RF/GBM on 15M rows, 2.2k cols [(github)](https://github.com/h2oai/h2o/commit/b01ea0d5b61d7687b0eae12f15fed3b54689a2b1)
- Updated R docs for SpeeDRF [(github)](https://github.com/h2oai/h2o/commit/e79c9589efad3d930dc05a69a34aeb4dc4e16db8)
- Added `class_sampling_factors` to GBM/DRF [(github)](https://github.com/h2oai/h2o/commit/cdd4cad0c748d19874c057f4a9d9aad3bc167588)

#####System 

- Added -extraJavaOpts option to the Hadoop H2O Driver [(github)](https://github.com/h2oai/h2o/commit/77bf6728a791fe4a7ad517ef1a88df9c0a755759)
- Background the deletion of ICE directory at startup [(github)](https://github.com/h2oai/h2o/commit/814c82217ff3c02cf72523fe7756e7fe1d163cd7)
- Added Hadoop options (as baseline) [(github)](https://github.com/h2oai/h2o/commit/e8c97e32f7ca1f5e3f6b1c295d1c77275db84e22)
- Added Hadoop cut and paste example for each distribution [(github)](https://github.com/h2oai/h2o/commit/8e224483df4db29e342abf35fe6f008d0affa2e4)
- Added R script for Next.ML [(github)](https://github.com/h2oai/h2o/commit/69c1d614d3a37e4428b516f2121d19caa3ded922)
- Use output of `boot2docker ip` instead of localhost [(github)](https://github.com/h2oai/h2o/commit/9687cd0b443e4ac23a772535bed2bd16e9bf25b7)


####Bug Fixes

These changes are to resolve incorrect software behavior: 

#####UI
- Fixed a bug in an example due to createFrame change [(github)](https://github.com/h2oai/h2o/commit/afc93a9734ea22efa9afcec46bbc7e26b2faee98)
- Import file hangs at 99% for past 45 mins [(HEX-2003)](https://0xdata.atlassian.net/browse/HEX-2003)

#####API
- as.h2o returns NA when character field has many unique values [(PUB-1108)](https://0xdata.atlassian.net/browse/PUB-1108)
- Fixed R for createFrame updates [(github)](https://github.com/h2oai/h2o/commit/f52f6a4679a2f5cacc0825c271f3c899cca43c03)

#####Algorithms
- Removed restrictions on glm upper/lower bounds to be non-negative/non-positive. The only restriction is lb <= ub [(github)](https://github.com/h2oai/h2o/commit/5980d62c436922e6c3c73cbfa0b83c1bcebff7d8)
- Fixed PUB-1118 - AutoEncoder POJO scoring. Also fixed a bug in initializing the activation layer for categoricals. [(github)](https://github.com/h2oai/h2o/commit/6505a1ffc0a99c211a6e14807ff280058ab9c0e7)
- Variable Importance for DRF slow for large numbers of columns [(PUB-1101)](https://0xdata.atlassian.net/browse/PUB-1101)
- Fix PUB-1101 by using GBM's variable importance method for DRF [(github)](https://github.com/h2oai/h2o/commit/0026200d4ba1e76f759cb4df5743555855b92c42)
- Bugfix for case where N is not divisible by num CV folds [(github)](https://github.com/h2oai/h2o/commit/34e271760b70fe6f384e106d84f18c7f0adb8210)
- Fixed typo in parameter validation [(github)](https://github.com/h2oai/h2o/commit/f6c6996594894c5ecdcd2ad112953b92844a3848)
- Don't shuffle intra-chunk after stratified sampling [(github)](https://github.com/h2oai/h2o/commit/f0ff6f027e4aa9e3faebcd5f192bd8781d47c5f1)
- Fixed intercept adjustment by prior probability in logistic regression [(github)](https://github.com/h2oai/h2o/commit/cdd8a6b54a35e83a5f9aebb419ada476593ab144)
- DL: DL build model running for past 12 hrs doesn't complete [(HEX-1997)](https://0xdata.atlassian.net/browse/HEX-1997)
- GLM fails simpleCheckGLM in multinode when cross-validation is on [(HEX-1867)](https://0xdata.atlassian.net/browse/HEX-1867)

#####System

- Parse creates too many keys for large numbers of columns [(PUB-1102)](https://0xdata.atlassian.net/browse/PUB-1102)
- Don't allow pure number enums [(github)](https://github.com/h2oai/h2o/commit/fb95e198db3a6186ef5ed408bd31f29dadae60a5)
- Fixed argument order [(github)](https://github.com/h2oai/h2o/commit/2a174063ca5ffdc4bd117a7206e45d89fa56317b)
- Excluded factors that contain lower-case UUIDs that look like they are in scientific notation with exponent: `^\d+e\d+$`[(github)](https://github.com/h2oai/h2o/commit/7fc6f1c23f254156a60ce343df4ca6b5b73f3156)
- Prevented deadlock during startup by adding UniqueFrameId to TypeMap [(github)](https://github.com/h2oai/h2o/commit/7e6fb375cd1c45c5a771b79add73f697409a312a)
- Fixed sparse chunk to avoid binary search for prefix/suffix zeros [(github)](https://github.com/h2oai/h2o/commit/48d4dfa9ad12461970c7d3c2f75907a27cb7d6f0)
