#----------------------------------------------------------------------
# Try to slice by using != factor_level
#----------------------------------------------------------------------

conn <- h2o.init(ip=myIP, port=myPort, startH2O=FALSE)

filePath <- "/home/0xdiag/datasets/airlines/airlines_all.csv"

# Uploading data file to h2o.
air <- h2o.importFile(conn, filePath, "air")

# Print dataset size.
dim(air)

#
# Example 1: Select all flights not departing from SFO
#

not.sfo <- air[air$Origin != "SFO",]
print(dim(not.sfo))

PASS_BANNER()
