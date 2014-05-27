
.h2o.__exec2 <- function(client, expr) {
  destKey = paste(.TEMP_KEY, ".", .pkg.env$temp_count, sep="")
  .pkg.env$temp_count = (.pkg.env$temp_count + 1) %% .RESULT_MAX
  .h2o.__exec2_dest_key(client, expr, destKey)
  # .h2o.__exec2_dest_key(client, expr, .TEMP_KEY)
}

.h2o.__exec2_dest_key <- function(client, expr, destKey) {
  type = tryCatch({ typeof(expr) }, error = function(e) { "expr" })
  if (type != "character")
    expr = deparse(substitute(expr))
  expr = paste(destKey, "=", expr)
  res = .h2o.__remoteSend(client, .h2o.__PAGE_EXEC2, str=expr)
  if(!is.null(res$response$status) && res$response$status == "error") stop("H2O returned an error!")
  res$dest_key = destKey
  return(res)
}

.h2o.unop<-
function(op, x) {
  if(missing(x)) stop("Must specify data set")
  if(!inherits(x, "H2OFrame")) stop(cat("\nData must be an H2O data set. Got ", class(x), "\n"))

  op <- new("ASTOp", type="UnaryOperator", operator=op, infix=FALSE)
  if (inherits(x, "ASTNode")) {
    return(new("ASTNode", root=op, children=list(arg=x)))
  } else if (inherits(x, "H2OParsedData")) {
   type_list <- list("Frame")
   type_defs <- list(x@key)
   names(type_list) <- names(type_defs) <- deparse(substitute(x))
   arg <- new("ASTFrame", s_table=list(types = type_list, defs = type_defs), type="Frame", value=x@key)
   return(new("ASTNode", root=op, children=list(arg=arg)))
  } else {
    stop("Data must be an H2O data set!")
  }
}

.h2o.binop<-
function(op, e1, e2) {

  #Case 1: l: ASTOp & r: Numeric
  if (inherits(e1, "ASTNode") && inherits(e2, "numeric")) {
    type_list <- list("ASTOp", "Numeric")  # An ASTNode is a root node. Root nodes are always ASTOps.
    type_defs <- list("", e2)
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- e1
    rhs <-  new("ASTNumeric", s_table=list(types = type_list, defs = type_defs), type="numeric", value=e2)
    op <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))

  #Case 2: l: ASTOp & r: ASTOp
  } else if (inherits(e1, "ASTNode") && inherits(e2, "ASTNode")) {
    type_list <- list("ASTOp", "ASTOp")  # An ASTNode is a root node. Root nodes are always ASTOps.
    type_defs <- list("", "")
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- e1
    rhs <- e2
    op <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))

  #Case 3: l: ASTOp & r: H2OParsedData
  } else if (inherits(e1, "ASTNode") && inherits(e2, "H2OParsedData")) {
    type_list <- list("ASTOp", "Frame")  # An ASTNode is a root node. Root nodes are always ASTOps.
    type_defs <- list("", e2@key)
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- e1
    rhs <- new("ASTFrame", s_table=list(types = type_list, defs = type_defs), type="Frame", value=e2@key)
    op <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))

  #Case 4: l: H2OParsedData & r: Numeric
  } else if (inherits(e1, "H2OParsedData") && inherits(e2, "numeric")) {
    type_list <- list("Frame", "Numeric")
    type_defs <- list(e1@key, e2)
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- new("ASTFrame", s_table=list(types = type_list, defs = type_defs), type="Frame", value=e1@key)
    rhs <- new("ASTNumeric", s_table=list(types = type_list, defs = type_defs), type="Numeric", value=e2)
    op <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))

  #Case 5: l: H2OParsedData & r: ASTOp
  } else if (inherits(e1, "H2OParsedData") && inherits(e2, "ASTNode")) {
    type_list <- list("Frame", "ASTOp")
    type_defs <- list(e1@key, "")
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- new("ASTFrame", s_table=list(types = type_list, defs = type_defs), type="Frame", value=e1@key)
    rhs <- e2
    op <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))

  #Case 6: l: H2OParsedData & r: H2OParsedData
  } else if (inherits(e1, "H2OParsedData") && inherits(e2, "H2OParsedData")) {
    type_list <- list("Frame", "Frame")
    type_defs <- list(e1@key, e2@key)
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- new("ASTFrame", s_table=list(types = type_list, defs = type_defs), type="Frame", value=e1@key)
    rhs <- new("ASTFrame", s_table=list(types = type_list, defs = type_defs), type="Frame", value=e2@key)
    op  <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))

  #Case 7: l: Numeric & r: ASTOp
  } else if (inherits(e1, "numeric") && inherits(e2, "ASTNode")) {
    type_list <- list("Numeric", "ASTOp")
    type_defs <- list(e1, "")
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- new("ASTNumeric", s_table=list(types = type_list, defs = type_defs), type="numeric", value=e1)
    rhs <- e2
    op <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))

  #Case 8: l: Numeric & r: H2OParsedData
  } else if (inherits(e1, "numeric") && inherits(e2, "H2OParsedData")) {
    type_list <- list("Numeric", "Frame")
    type_defs <- list(e1, e2@key)
    names(type_list) <- names(type_defs) <- c(deparse(substitute(e1)), deparse(substitute(e2)))
    lhs <- new("ASTNumeric", s_table=list(types = type_list, defs = type_defs), type="numeric", value=e1)
    rhs <- new("ASTFrame", s_table=list(types = type_list, defs = type_defs), type="Frame", value=e2@key)
    op <- new("ASTOp", type="BinaryOperator", operator=op, infix=TRUE)
    return(new("ASTNode", root=op, children=list(left = lhs, right = rhs)))
  }
}
