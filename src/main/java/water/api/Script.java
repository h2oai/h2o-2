package water.api;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.*;

import water.*;
import water.util.*;
import water.util.Log.LogStr;
import water.util.Log.Tag.Sys;

import com.amazonaws.util.StringInputStream;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Script extends Request {
  // How many requests
  protected final Int _count = new Int(COUNT, 100);
  // Flag to notify a Hadoop job we are done (c.f. deploy.Hadoop)
  public static final String DONE = "_done";
  public static volatile boolean _done;

  @Override protected String href() {
    return "script";
  }

  @Override protected RequestType hrefType() {
    return RequestType.txt;
  }

  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    checkArguments(args, type);
    int count = _count.value();

    switch( type ) {
      case txt: {
        LogStr logstr = UKV.get(Log.LOG_KEY);
        String script = "";
        String nl = System.getProperty("line.separator");
        if( logstr != null ) {
          for( int i = 0; i < LogStr.MAX; i++ ) {
            int x = (i + logstr._idx + 1) & (LogStr.MAX - 1);
            if( logstr._dates[x] == null ) continue;
            if( Log.SYSS[logstr._syss[x]] == Sys.HTTPD ) {
              script += logstr._msgs[x];
              script += nl + nl;
            }
            if( --count == 0 ) break;
          }
        }
        InputStream in;
        try {
          in = new StringInputStream(script);
        } catch( UnsupportedEncodingException e ) {
          throw Log.errRTExcept(e);
        }
        // Unknown mime prompts "Save As" of the script
        return server.new Response(NanoHTTPD.HTTP_OK, "application/x-unknown", in);
      }
      default:
        throw new RuntimeException();
    }
  }

  @Override protected boolean log() {
    return false;
  }

  @Override protected Response serve() {
    throw new RuntimeException();
  }

  public static class RunScript extends Request {
    protected final H2OExistingKey _key = new H2OExistingKey(KEY);

    @Override protected boolean log() {
      return false;
    }

    @Override protected Response serve() {
      String line = null;
      try {
        ValueArray va = _key.value().get();
        ByteBuffer bb = va.getChunk(0)._bb;
        String script = new String(bb.array(), 0, bb.remaining(), "UTF-8");
        StringTokenizer tok = new StringTokenizer(script, "\n\r");
        String cmd = null;
        Properties args = new Properties();
        for( ;; ) {
          if( cmd == null ) {
            line = next(tok);
            if( line == null ) break;
            Log.debug(Sys.HTTPD, "RunScript: " + line);
            String[] a = line.split(" ");
            cmd = a[0];
            assert args.size() == 0;
            for( int i = 1; i < a.length; i++ ) {
              String[] arg = a[i].split("=");
              args.put(arg[0], arg[1]);
            }
          }
          Request request = RequestServer._requests.get(cmd);
          if( request == null ) throw new Exception("Unknown command: " + line);
          request.checkArguments(args, RequestType.json);
          Response r = request.serve();
          switch( r._status ) {
            case error:
              return Response.error(r.error() + ", " + line);
            case redirect:
              cmd = r._redirectName;
              args.clear();
              for( Entry<String, JsonElement> entry : r._redirectArgs.entrySet() )
                args.put(entry.getKey(), entry.getValue().getAsString());
              break;
            case poll:
              // Not a FJ thread, just wait
              Thread.sleep(100);
              break;
            case done:
              cmd = null;
              args.clear();
              break;
          }
        }
        for( H2ONode node : H2O.CLOUD._memary )
          RPC.call(node, new Done());
        return Response.done(new JsonObject());
      } catch( Exception ex ) {
        return Response.error(ex.getMessage() + ", " + line);
      }
    }

    private static String next(StringTokenizer t) {
      for( ;; ) {
        if( !t.hasMoreElements() ) return null;
        String line = t.nextToken();
        if( !line.startsWith("#") ) return line;
      }
    }
  }

  private static class Done extends DTask {
    @Override public void compute2() {
      _done = true;
    }
  }
}
