package water.exec3;


import com.google.gson.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;


/**
 *  Transform the high-level AST passed from R into ymbol tables along with an instruction set.
 *
 *  Walk the R-AST and produce execution instructions and symbol tables to be passed into an object of type Env. There
 *  are at most two symbol tables for every scope (i.e. every Env represents its own scope, see Env.java for more):
 *    1. The Global symbol table;
 *    2. The Local  symbol table;
 *  The symbol table class explains this data structure in more detail.
 *
 *  More Details:
 *  -------------
 *  The Intermediate Representation of the R expression:
 *
 *  The first phase of executing the R expression occurs in the "front-end" R session, where all operations and calls
 *  are intercepted by the h2o R package and subsequently "packed away" into abstract syntax trees. These ASTs are
 *  converted to JSON, sent over the wire, and dropped here for further analysis.
 *
 *  First cut: Don't do any analysis or java code generation. Generate simple instructions for the interpreter.
 *
 *  The POJO returned from here is passed to an instance of Env. The POJO has is a map of programs to be executed:
 *      AST2IR.getInstructionSet() will return a new HashMap<String, Program>(), where a Program is a list of
 *      instructions plus the accompanying symbol tables.
 *
 *  There is a special type of program that is the "global" program. This is the __main__ program. It is responsible for
 *  switching control to other programs (user-defined functions and other calls), and managing returned values. There
 *  is always a main, even in cases such as result <- f(...). The left-arrow assignment is the main in this case.
 *  If f(...) is called without assignment, then there is a temporary key created and spit out to the console. The
 *  lifetime of this temporary key is discussed elsewhere.
 */
public class AST2IR {


//  static AST parseOp(JsonObject jo) {
//
//    // What type of operator is it?
//    if (jo.get("type").getAsString().equals("BinaryOperator")) {
//
//      // Parse the left and right operands for this operator and return.
//      JsonObject operands = jo.get("operands").getAsJsonObject();
//      JsonObject left = operands.get("left").getAsJsonObject();
//      JsonObject rite = operands.get("right").getAsJsonObject();
//
//
//
//    } else if (jo.get("type").getAsString().equals("PrefixOperator")) {
//
//    } else {
//      throw new IllegalArgumentException("Unkown operator type: " + jo.get("type").getAsString());
//    }
//    return ast;
//  }


  JsonObject _ast;
  SymbolTable _global;
  String[] arithmeticOps = new String[]{"+", "-", "/", "*"};
  String[] bitwiseOps = new String[]{"&", "&&", "|", "||", "!"};
  String[] compareOps = new String[]{"!=", "<=", "==", ">=", "<", ">"};

  //Arrays.asList(...).contains(...)

  public AST2IR(JsonObject ast) {
    _ast = ast;
    _global = new SymbolTable();
  }

  private boolean isArithmeticOp(JsonObject node) {
    if (isOp(node)) {
      JsonObject jo = node.get("astop").getAsJsonObject();
      if (jo.get("type").getAsString().equals("BinaryOperator")) {
        if (Arrays.asList(arithmeticOps).contains(jo.get("operator").getAsString()))
          return true;
      }
    }
    return false;
  }

  private boolean isBitwiseOp(JsonObject node) {
    if (isOp(node)) {
      JsonObject jo = node.get("astop").getAsJsonObject();
      if (jo.get("type").getAsString().equals("BinaryOperator")) {
        if (Arrays.asList(bitwiseOps).contains(jo.get("operator").getAsString()))
          return true;
      }
    }
    return false;
  }

  private boolean isCompareOp(JsonObject node) {
    if (isOp(node)) {
      JsonObject jo = node.get("astop").getAsJsonObject();
      if (jo.get("type").getAsString().equals("BinaryOperator")) {
        if (Arrays.asList(compareOps).contains(jo.get("operator").getAsString()))
          return true;
      }
    }
    return false;
  }

  private boolean isCall(String f) { }

  private boolean isId() { }

  private boolean isConst() { }

  private boolean isString() { }

  private boolean isArg() { }

  private boolean isOp(JsonObject node) {
    return node.get("astop") != null;
  }
}

/**
 *  The Symbol Table Data Structure: A mapping between identifiers and their values.
 *
 *  The role of the symbol table is to track the various identifiers and their attributes that appear in the nodes of
 *  the AST passed from R. There are three cases that are of interest:
 *    1. The identifier is a variable that references some blob of data having a type, value, and scope.
 *    2. The identifier is part of an assignment and its type is whatever the type is on the left-hand side.
 *    3. There is no identifier: a non-case, but worth mentioning.
 *
 *  As already stated, each identifier has a name, type, value, and scope. The scoping is implied by the Env object,
 *  so it is not necessary to include this attribute.
 *
 *  Valid types:
 *
 *    Usual types: string, int, double, float, boolean,
 *    unk: An identifier being assigned to
 *    arg: An argument to a function call
 *    call: A function call (if UDF, value is the body of the function)
 *    key: An h2o key
 *
 *  Symbol Table Permissions:
 *  -------------------------
 *
 *  Every Program object will have at most two symbol tables: the global and local tables.
 *  The global table is read only by every Program that has a non-null local table.
 *  The global table is read-write by the main program only.
 *
 *  NB: The existence of a non-null symbol table implies that operation is occurring in a non-global scope.
 */
class SymbolTable {

  HashMap<String, SymbolAttributes> _table;
  SymbolTable() { _table = new HashMap<String, SymbolAttributes>(); }

  public void put(String name, String type, String value) {
    if (_table.containsKey(name)) { return; }
    SymbolAttributes attributes = new SymbolAttributes(type, value);
    _table.put(name, attributes);
  }

  public String typeOf(String name) {
    assert _table.containsKey(name) : "No such identifier in the symbol table: " + name;
    return _table.get(name).typeOf();
  }

  public String valueOf(String name) {
    assert _table.containsKey(name) : "No such identifier in the symbol table: " + name;
    return _table.get(name).valueOf();
  }

  private class SymbolAttributes {
    private String _type;
    private String _value;

    SymbolAttributes(String type, String value) { _type = type; _value = value; }

    public String typeOf ()  { return _type;  }
    public String valueOf() { return _value; }
  }
}