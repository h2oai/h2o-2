package water.r.commands;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.data.RNull;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.*;
import water.ValueArray.Column;
import water.util.Log;
import water.util.Utils;

public class Save implements Invokable {
  public String name() { return "save"; }

  public String[] parameters() { return new String[] { "uri", "key" }; }

  public String[] requiredParameters() { return new String[] { "uri", "key" }; }

  public RAny invoke(ArgumentInfo ai, RAny[] args) {
    FileWriter w = null;
    try {
      URI uri = new URI(Interop.asString(args[0]));
      if( uri.getScheme() == null || uri.getScheme().equals("file") ) {
        w = new FileWriter(new File(uri.getPath()));
        Key key = Key.make(Interop.asString(args[1]));
        ValueArray va = DKV.get(key).get();
        for(long r = 0; r < va._numrows; r++) {
          long chknum = va.chknum(r);
          AutoBuffer bits = va.getChunk(chknum);
          int rowInChunk = va.rowInChunk(chknum, r);
          String line = "";
          for( Column c : va._cols ) {
            if(line.length() != 0) line += ", ";
            line += va.datad(bits, rowInChunk, c);
          }
          w.write(line + '\n');
        }
      } else if( uri.getScheme().equals("hdfs") ) {
        //
      }
      return RNull.getNull();
    } catch( Exception e ) {
      throw Log.errRTExcept(e);
    } finally { Utils.close(w); }
  }
}