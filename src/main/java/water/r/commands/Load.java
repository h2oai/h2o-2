package water.r.commands;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.data.RNull;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.TestUtil;
import water.util.Log;

// FIXME:  why do we need this one?
public class Load implements Invokable {
  public String name() { return "load"; }

  public String[] parameters() { return new String[] { "uri" }; }

  public String[] requiredParameters() { return new String[] { "uri" }; }

  public RAny invoke(ArgumentInfo ai, RAny[] args) {
    URI uri;
    try {
      uri = new URI(Interop.asString(args[0]));
    } catch( URISyntaxException e ) {  throw Log.errRTExcept(e);  }
    if( uri.getScheme() == null || uri.getScheme().equals("file") ) {
      File f = new File(uri.getPath());
      return Interop.asRString(TestUtil.load_test_file(f, f.getName()).toString());
    }
    if( uri.getScheme().equals("hdfs") ) {
      //
    }
    return RNull.getNull();
  }
}