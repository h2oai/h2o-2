setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source('../../findNSourceUtils.R')


test.eq2.h2o.exec<-
function(conn) {
    hex <- h2o.uploadFile(conn, locate("smalldata/iris/iris.csv"), "iris.hex")

    Log.info("Print out the head of the iris dataset")
    print(hex)
    print(head(hex))

    Log.info("Add together the first two columns of iris and store in 'res1'")

    res1 <- h2o.exec(conn, hex[,1] + hex[,2], "res1")
    
    Log.info("Here's the result of res1")
    print(head(res1))

    r_res1 <- iris[,1] + iris[,2]
    print(head(r_res1))
    print(head(res1)[,1])
    expect_that(head(res1)[,1], equals(head(r_res1)))

    Log.info("Try a more complicated expression:")
    Log.info("Trying: hex[,1] + hex[, 2] + hex[, 3] * hex[,4] / hex[,1]")

    res2 <- h2o.exec(client = conn, expr_to_execute= hex[,1] + hex[, 2] + hex[, 3] * hex[,4] / hex[,1], destination_key= "res2")
   
    print(head(res2))
    
    r_res2 <- iris[,1] + iris[,2] + iris[,3] * (iris[,4] / iris[,1])
    expect_that(head(res2)[,1], equals(head(r_res2)))

    Log.info("Try intermixing scalars: hex[,1] + hex[, 2] + hex[, 3] + (hex[,4] / 2) - (hex[,2] / hex[,1]) * 12.3")
    
    res3 <- h2o.exec(client = conn, expr_to_execute= hex[,1] + hex[, 2] + hex[, 3] + (hex[,4] / 2) - (hex[,2] / hex[,1]) * 12.3, destination_key= "res3")
    print(head(res3))

    r_res3 <- iris[,1] + iris[,2] + iris[,3] + (iris[,4] / 2) - (iris[,2] / iris[,1]) * 12.3
    expect_that(head(res3)[,1], equals(head(r_res3)))

    Log.info("Multiple column selection")
    
    res4 <- h2o.exec(client = conn, expr_to_execute = hex[,c(1,2)] + hex[,c(2,4)], destination_key = "res4")
    r_res4 <- iris[,c(1,2)] + iris[,c(2,4)]

    print(head(res4))
    expect_that(dim(res4), equals(dim(r_res4)))
    colnames(r_res4) <- paste("C", 1:dim(r_res4)[2], sep = "")
    print(head(r_res4))
    print(head(as.data.frame(res4)))
    expect_that(head(as.data.frame(res4)), equals(head(r_res4)))

    res5 <- h2o.exec(client = conn, expr_to_execute = hex[,seq(1,4,1)] + hex[,seq(1,4,1)], destination_key = "res4")
    r_res5 <- iris[,1:4] + iris[,1:4]

    print(head(res5))
    expect_that(dim(res5), equals(dim(r_res5)))
    colnames(r_res5) <- paste("C", 1:dim(r_res5)[2], sep = "")
    print(head(r_res5))
    print(head(as.data.frame(res5)))
    expect_that(head(as.data.frame(res5)), equals(head(r_res5)))
  

    testEnd()
}

doTest("Test h2o.exec(client, expr, key)", test.eq2.h2o.exec)

