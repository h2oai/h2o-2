setClass("H2OFVRawData", representation(h2o="H2OClient", key="character"))
setClass("H2OFVParsedData", representation(h2o="H2OClient", key="character"))

setGeneric("h2o.FV.importFile", function(object, path, parse = TRUE) { standardGeneric("h2o.FV.importFile") })
setGeneric("h2o.FV.parseRaw", function(data) { standardGeneric("h2o.FV.parseRaw") })
setGeneric("h2o.FV.inspect", function(data, offset = 0) { standardGeneric("h2o.FV.inspect") })
setGeneric("h2o.FV.GBM", function(y, data, ntree = 10, learning.rate = 0.1, max.depth = 8) { standardGeneric("h2o.FV.GBM") })

setMethod("h2o.FV.importFile", signature(object="H2OClient", path="character", parse="logical"),
          function(object, path, parse) {
            res = h2o.__remoteSend(object, h2o.__PAGEFV_IMPORTFILE, path=path)
            res$destination_key = paste("nfs:/", path, sep="")
            rawData = new("H2OFVRawData", h2o=object, key=res$destination_key)
            if(parse) h2o.FV.parseRaw(rawData)
            else rawData
          })

#
setMethod("h2o.FV.importFile", signature(object="H2OClient", path="character", parse="missing"),
          function(object, path) { h2o.FV.importFile(object, path, parse = TRUE) })

setMethod("h2o.FV.parseRaw", signature(data="H2OFVRawData"), function(data) {
  res = h2o.__remoteSend(data@h2o, h2o.__PAGEFV_PARSE, source_key=data@key, blocking=1)
  # while(h2o.__poll(data@h2o, res$response$redirect_request_args$job) != -1) { Sys.sleep(1) }
  g = regexpr("\\.[^\\.]*$", data@key)
  destKey = paste(substring(data@key, 1, g[1]), "hex", sep="")
  # parsedData = new("H2OFVParsedData", h2o=data@h2o, key=res$destination_key)
  parsedData = new("H2OFVParsedData", h2o=data@h2o, key=destKey)
})

setMethod("h2o.FV.inspect", signature(data="H2OFVParsedData", offset="numeric"), function(data, offset) {
  res = h2o.__remoteSend(data@h2o, h2o.__PAGEFV_INSPECT, src_key=data@key, offset=offset)
  rnames = lapply(res$cols, function(x) { x$name })
  cnames = c("name", "min", "max", "mean", "NAcnt", "type")
  temp = data.frame(matrix(unlist(res$cols), ncol = length(res$cols[[1]]), byrow = TRUE, dimnames = list(rnames, cnames)))
  temp[,2:5] = sapply(temp[,2:5], function(x) { as.numeric(as.character(x)) })
  temp[,-1]
})

setMethod("h2o.FV.inspect", signature(data="H2OFVParsedData", offset="missing"), 
          function(data) { h2o.FV.inspect(data, offset=0) })

setMethod("h2o.FV.GBM", signature(y="character", data="H2OFVParsedData", ntree="numeric", learning.rate="numeric", max.depth="numeric"),
          function(y, data, ntree, learning.rate, max.depth) {
            res = h2o.__remoteSend(data@h2o, h2o.__PAGEFV_GBM, vresponse=y, source=data@key, ntrees=ntree, learning_rate=learning.rate, max_depth=max.depth, destination_key=paste(data@key, "gbm", sep="."))
            result = list()
            result$train.err = res$errs
            result$confusion = res$cm
            result$confusion = t(matrix(unlist(res$cm), nrow = length(res$cm)))
            dimnames(result$confusion) = list(Actual = res$domain, Predicted = res$domain)
            result
          })

setMethod("h2o.FV.GBM", signature(y="character", data="H2OFVParsedData", ntree="ANY", learning.rate="ANY", max.depth="ANY"),
          function(y, data, ntree, learning.rate, max.depth) {
            (!(missing(ntree) || is.numeric(ntree)))
            stop(paste("ntree cannot be of class", class(key)))
            if(!(missing(learning.rate) || is.numeric(learning.rate)))
              stop(paste("learning.rate cannot be of class", class(learning.rate)))
            if(!(missing(max.depth) || is.numeric(max.depth)))
              stop(paste("max.depth cannot be of class", class(max.depth)))
            h2o.FV.GBM(y=y, data=data, ntree=ntree, learning.rate=learning.rate, max.depth=max.depth)
          })

setMethod("h2o.FV.GBM", signature(y="numeric", data="H2OFVParsedData", ntree="ANY", learning.rate="ANY", max.depth="ANY"),
          function(y, data, ntree, learning.rate, max.depth) { h2o.FV.GBM(y=as.character(y), data, ntree, learning.rate, max.depth)  })

h2o.__PAGEFV_IMPORTFILE = "ImportFiles2.json"
h2o.__PAGEFV_PARSE = "Parse2.json"
h2o.__PAGEFV_INSPECT = "Inspect2.json"
h2o.__PAGEFV_GBM = "GBM.json"
