package water.r.commands;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.data.RNull;
import r.ifc.Interop.Invokable;

public class Shutdown implements Invokable {

  @Override public RAny invoke(ArgumentInfo _0, RAny[] _1) {
    new water.api.Shutdown().serve();
    return RNull.getNull();
  }

  @Override public String name() { return "shutdown";  }
  @Override public String[] parameters() { return new String[]{}; }
  @Override public String[] requiredParameters() { return new String[]{}; }
}