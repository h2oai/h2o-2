package water.exec3;


import java.util.ArrayList;
import java.util.Iterator;

/**
 * The Program class: A unit of work to be executed by Env.
 *
 * A program can lookup the types and values of identifiers.
 * It may also write to the symbol table if it has permission.
 *
 * Valid operations are:
 *
 *  >push:   Push a blob of data onto the stack.
 *  >pop:    Pop a blob of data from the stack.
 *  >dup:    Push a copy of the data blob on the top of the stack back onto the stack.
 *  >op:     Do some operator specific instruction (handles all binary & prefix operators)
 *  >call:   Do some function call (handles all user-defined functions)
 *  >return: Return the result of the program (possibly to some return address)
 */
public class Program implements Iterable<Program.Statement>{
  private SymbolTable _global; // The global symbol table.
  private SymbolTable _local;  // The local symbol table, null if program is main.
  private boolean     _isMain; // A boolean stating that this program is main.
  private String      _name;   // The program's name.
  private ArrayList<Statement> _stmts;  // The list of program statements

  public static final class Statement<T> {
    String _op;    // One of the valid statement operations: push, pop, dup, op, call, return
    T _name;       // The name (can be null) of some data blob used by _op (can be a Key, String, Double, Boolean, etc.)

    Statement(String op, T name) {
      _op = op;
      _name = name;
    }
  }

  Program(SymbolTable global, SymbolTable local, String name) {
    _global = global;
    _local  = local;
    _isMain = _local == null;
    _name   = name;
    _stmts  = new ArrayList<Statement>();
  }

  public String name() { return _name; }
  protected int start() { return 0; }
  protected int end() { return _stmts.size(); }
  protected final boolean isMain() { return _isMain; }
  protected final boolean canWriteToGlobal() { return isMain(); }

  protected final String readType(String name) {
    if (_local != null) {
      if (_local.typeOf(name) != null) return _local.typeOf(name);
    }
    if (_global.typeOf(name)!= null) return _global.typeOf(name);
    throw new IllegalArgumentException("Could not find the identifier in the local or global scope while looking up type of: "+name);
  }

  protected final String readValue(String name) {
    if (_local != null) {
      if (_local.valueOf(name) != null) return _local.valueOf(name);
    }
    if (_global.valueOf(name)!= null) return _global.valueOf(name);
    throw new IllegalArgumentException("Could not find the identifier in the local or global scopes while looking up value of: "+name);
  }

  // These write methods will stomp on the attributes for identifiers in the symbol table.
  protected final void writeType(String id, String type) {
    if (canWriteToGlobal()) {
      _global.writeType(id, type);
    } else {
      _local.writeValue(id, type);
    }
  }

  protected final void writeValue(String id, String value) {
    if (canWriteToGlobal()) {
      _global.writeValue(id, value);
    } else {
      _local.writeValue(id, value);
    }
  }

  protected final void putToTable(String name, String type, String value) {
    if (canWriteToGlobal()) {
      _global.put(name, type, value);
    } else {
      _local.put(name, type, value);
    }
  }

  protected final void addStatement(Statement stmt) { _stmts.add(stmt); }

  protected Statement getStmt(int idx) {return _stmts.get(idx); }

  public final Iterator<Statement> iterator() { return new StatementIter(start(), end()); }
  private class StatementIter implements Iterator<Statement> {
    int _pos = 0; final int _end;
    public StatementIter(int start, int end) { _pos = start; _end = end;}
    public boolean hasNext() { return _pos < _end;}
    public Statement next() { return getStmt(_pos++); }
    public void remove() { throw new RuntimeException("Unsupported"); }
  }
}