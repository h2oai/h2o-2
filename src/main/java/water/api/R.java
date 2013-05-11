package water.api;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.*;
import r.data.RArray.Names;
import r.data.RList.RListFactory;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.TestUtil;
import water.api.RequestArguments.Argument;
import water.api.RequestBuilders.Response;
import water.api.RequestStatics.RequestType;
import water.util.Log;

import com.google.gson.*;

public class R {
  public static void init() {
    for( final Request request : RequestServer._requests.values() ) {
      Interop.register(new Load());

      Interop.register(new Invokable() {
        @Override public String name() {
          return request.getClass().getSimpleName().toLowerCase();
        }

        @Override public String[] parameters() {
          ArrayList<String> res = new ArrayList<String>();
          for( Argument a : request._arguments )
            res.add(a._name);
          return res.toArray(new String[0]);
        }

        @Override public String[] requiredParameters() {
          ArrayList<String> res = new ArrayList<String>();
          for( Argument a : request._arguments )
            if( a._required ) res.add(a._name);
          return res.toArray(new String[0]);
        }

        @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
          for( Argument a : request._arguments )
            a.reset();
          for( Argument a : request._arguments ) {
            int pos = ai.position(a._name);
            if( pos >= 0 && !a.disabled() ) {
              try {
                a.check(Interop.asString(args[pos].asString()));
              } catch( IllegalArgumentException e ) {
                throw Log.errRTExcept(e);
              }
            }
          }
          return exec(request);
        }
      });
    }
  }

  private static RAny exec(Request request) {
    for( ;; ) {
      Response r = request.serve();
      switch( r._status ) {
        case error:
          throw new RuntimeException(r.error());
        case redirect:
          request = RequestServer._requests.get(r._redirectName);
          Properties args = new Properties();
          for( Entry<String, JsonElement> entry : r._redirectArgs.entrySet() )
            args.put(entry.getKey(), entry.getValue().getAsString());
          request.checkArguments(args, RequestType.json);
          break;
        case poll:
          // Not a FJ thread, just wait
          try {
            Thread.sleep(100);
          } catch( InterruptedException e ) {
            throw new RuntimeException(e);
          }
          break;
        case done:
          return toR(r._response);
      }
    }
  }

  private static RAny toR(JsonElement e) {
    if( e instanceof JsonObject ) {
      Set<Entry<String, JsonElement>> set = ((JsonObject) e).entrySet();
      RAny[] a = new RAny[set.size()];
      RSymbol[] names = new RSymbol[set.size()];
      int count = 0;
      for( Entry<String, JsonElement> entry : set ) {
        a[count] = toR(entry.getValue());
        names[count++] = RSymbol.getSymbol(entry.getKey());
      }
      return RListFactory.getFor(a, new int[] { 1 }, Names.create(names));
    }
    if( e instanceof JsonArray ) {
      RAny[] a = new RAny[((JsonArray) e).size()];
      for( int i = 0; i < a.length; i++ )
        a[i] = toR(((JsonArray) e).get(i));
      return RListFactory.getFor(a);
    }
    return Interop.asRString(e.getAsString());
  }

  private static class Load implements Invokable {
    @Override public String name() {
      return "load";
    }

    @Override public String[] parameters() {
      return new String[] { "uri" };
    }

    @Override public String[] requiredParameters() {
      return new String[] { "uri" };
    }

    @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
      URI uri;
      try {
        uri = new URI(Interop.asString(args[0]));
      } catch( URISyntaxException e ) {
        throw Log.errRTExcept(e);
      }
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
}
