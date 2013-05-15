package water.r.commands;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.ifc.Interop.Invokable;
import water.ValueArray;

/**
 * The R version of parsing.
 *
 * The parse command is currently designed to be blocking and to do both import and parse at the same time.
 */
public class Parse implements Invokable {
  public String name() { return "h2o.parse"; }

  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

  @Override public String[] requiredParameters() {
    throw new RuntimeException("TODO Auto-generated method stub");
  }

  @Override public String[] parameters() {
    throw new RuntimeException("TODO Auto-generated method stub");
  }


  /**
   * The execute method does the work. Eventually it will have to take all the parse arguments.
   * But right now it is at its simplest.
   */
  ValueArray execute(URI[] names) {

    return null;
  }

}
