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
  } else {
    .ASTToList(node)
  }
}