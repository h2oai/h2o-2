#'
#' The AST visitor method.
#'
#' This method represents a map between an AST S4 object and a regular R list,
#' which is suitable for the rjson::toJSON method.
#'
#' Given a node, the `visitor` function recursively "list"-ifies the node's S4 slots and then returns the list.
#'
#' A node that has a "root" slot is an object of type ASTOp. An ASTOp will always have a "children" slot representing
#' its operands. A root node is the most general type of input, while an object of type ASTFrame or ASTNumeric is the
#' most general. This method relies on the private helper function .ASTToList(...) to map the AST S4 object to a list.


#'
#' Retrieve the slot value from the object given its name and return it as a list.
.slots<-
function(name, object) {
    ret <- list(slot(object, name))
    names(ret) <- name
    ret
}

#'
#' Cast an S4 AST object to a list.
#'
#'
#' For each slot in `object`, create a list entry of name "slotName", with value given by the slot.
#'
#' To unpack this information, .ASTToList depends on a secondary helper function `.slots(...)`.
#' Finally, the result of the lapply is unlisted a single level, such that a vector of lists is returned
#' rather than a list of lists. This helps avoids anonymous lists.
.ASTToList<-
function(object) {
  return( unlist(recursive = FALSE, lapply(slotNames(object), .slots, object)))
}

#'
#' The AST visitor method.
#'
#' This method represents a map between an AST S4 object and a regular R list,
#' which is suitable for the rjson::toJSON method.
visitor<-
function(node) {
  if (.hasSlot(node, "root")) {
    root_values <- .ASTToList(node@root)
    children <- lapply(node@children, visitor)
    root_values$operands <- children
    list(astop = root_values)
  } else if (.hasSlot(node, "statements")) {
    f_name <- node@name
    children <- lapply(node@statements, visitor)
    l <- list(children)
    names(l) <- f_name
    l
  } else {
    .ASTToList(node)
  }
}

#'
#' Check if the call is user defined.
#'
#' A call is user defined if its environment is the Global one.
.isUDF<-
function(fun) {
  e <- environment(eval(fun))
  identical(e, .GlobalEnv)
}

#'
#' Check if operator is infix.
#'
#' .INFIX_OPERATORS is defined in cosntants.R. Used by .exprToAST.
.isInfix<-
function(o) {
  o %in% .INFIX_OPERATORS
}

#'
#' Return the class of the eval-ed expression.
#'
#' A convenience method for lapply. Used by .exprToAST
.evalClass<-
function(i) { class(eval(i)) }

#'
#' Walk the R AST directly.
#'
#' This walks the AST for some arbitrary R expression and produces an "S4"-ified AST.
#'
#' This function has lots of twists and turns mainly for involving h2o S4 objects.
#' We have to "prove" that we can safely eval an expression by showing that the
#' intersection of the classes in the expression with a vector of some of the H2O object types is non empty.
#'
#' The calls to eval here currently redirect to .h2o.binop() and .h2o.unop(). In the future,
#' this call may redirect to .h2o.varop() to handle multiple arg methods.
.exprToAST<-
function(expr) {

  # Assigning to the symbol
  if (is.symbol(expr)) {
    return( new("ASTUnk", key=as.character(expr)))
  }

  # Got an atomic numeric. Plain old numeric value. #TODO: What happens to logicals?
  if (is.atomic(expr) && class(expr) == "numeric") {
    new("ASTNumeric", type="numeric", value=expr)

  # Got an atomic string. #TODO: What to do with print/cat statements in h2o? Ignore them in UDFS?
  } else if (is.atomic(expr) && class(expr) == "character") {
    new("ASTString", type="character", value=expr)

  # Got a left arrow assignment statement
  } else if (identical(expr[[1]], quote(`<-`))) {
    lhs <- new("ASTUnk", key=as.character(expr[[2]]))
    rhs <- .exprToAST(expr[[3]])
    op <- new("ASTOp", type="LAAssignOperator", operator="<-", infix=TRUE)
    new("ASTNode", root=op, children=list(left = lhs, right = rhs))

  # Got an equals assignment statement
  } else if (identical(expr[[1]], quote(`=`))) {
    lhs <- new("ASTUnk", key=as.character(expr[[2]]))
    rhs <- .exprToAST(expr[[3]])
    op <- new("ASTOp", type="EQAssignOperator", operator="=", infix=TRUE)
    new("ASTNode", root=op, children=list(left = lhs, right = rhs))

  # Got a named function
  } else if (is.name(expr[[1]])) {
    o <- deparse(expr[[1]])

    # Is the function generic? (see getGenerics() in R)
    if (isGeneric(o)) {

      # Prove that we have h2o infix:
      if (.isInfix(o)) {
        if (length( intersect(c("H2OParsedData", "H2OFrame", "ASTNode"), unlist(lapply(expr, .evalClass)))) > 0) {

          # Calls .h2o.binop
          return(eval(expr))
        } else {

          # Regular R infix... recurse down the left and right arguments
          op <- new("ASTOp", type="InfixOperator", operator=as.character(expr[[1]]), infix=TRUE)
          args <- lapply(expr[-1], .exprToAST)
          return(new("ASTNode", root=op, children=args))
        }
      # Function is not infix, but some prefix method. #TODO: Must ensure a _single_ argument here: No .h2o.varop yet!
      } else {

        # Prove that we have h2o prefix:
        if (length( intersect(c("H2OParsedData", "H2OFrame", "ASTNode"), unlist(lapply(expr, .evalClass)))) > 0) {

          # Calls .h2o.unop
          return(eval(expr))
       }
      }
    }
    # Not an R generic, operator is some other R method. Recurse down the arguments.
    op <- new("ASTOp", type="PrefixOperator", operator=as.character(expr[[1]]), infix=FALSE)
    args <- lapply(expr[-1], .exprToAST)
    new("ASTNode", root=op, children=args)

  # Got an H2O object back: Must inherit from H2OFrame or error (NB: ASTNode inherits H2OFrame)
  } else if (is.object(expr)) {
    if (inherits(expr, "ASTNode")) {
      expr
    } else if (inherits(expr, "H2OFrame")) {
      new("ASTFrame", type="Frame", value=expr@key)
    } else {
      stop("Unfamiliar object. Got: ", class(expr), " Unimplemented.")
    }
  }
}

#'
#' Helper function for .funToAST
#'
#' Recursively discover other user defined functions and hand them to .funToAST.
#' Hand *real* R expressions over to .exprToAST.
.funToASTHelper<-
function(piece) {
  f_call <- piece[[1]]

  # Check if user defined function
  if (.isUDF(f_call)) {

    # Keep a global eye on functions we have definitions for to prevent infinite recursion
    if (! (any(f_call == .pkg.env$call_list))) {
      .pkg.env$call_list <- c(.pkg.env$call_list, f_call)
      .funToAST(eval(f_call))
    }
  } else {
    .exprToAST(piece)
  }
}

#'
#' Translate a function's body to an AST.
#'
#' Recursively build an AST from a UDF.
.funToAST<-
function(fun) {
  if (!.isUDF(fun)) {
    l <- as.list(body(fun))
    statements <- lapply(l[-1], .funToASTHelper)
    .pkg.env$call_list <- NULL
    new("ASTFun", type="UDF", name=deparse(substitute(fun)), statements=statements)
  }
}
