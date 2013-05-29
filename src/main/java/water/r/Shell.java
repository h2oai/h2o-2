package water.r;

import java.util.*;
import java.util.Map.Entry;

import r.Console;
import r.builtins.CallFactory.ArgumentInfo;
import r.data.*;
import r.data.RArray.Names;
import r.data.RList.RListFactory;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.api.*;
import water.api.RequestArguments.Argument;
import water.r.commands.*;
import water.r.commands.Parse;
import water.util.Log;

import com.google.gson.*;

public class Shell extends Thread {
  public Shell() {
    super("Shell");
  }

  public static void go() {
    Shell shell = new Shell();
    shell.start();
  }

  @Override public void run() {
    run(new String[0]);
  }

  public static void run(String[] args) {
    for( final Request request : RequestServer.requests().values() ) {
      Interop.register(new Invokable() {
        @Override public String name() {
          return request.getClass().getSimpleName().toLowerCase();
        }

        @Override public String[] parameters() {
          ArrayList<String> res = new ArrayList<String>();
          for( Argument a : request.arguments() )
            res.add(a._name);
          return res.toArray(new String[0]);
        }

        @Override public String[] requiredParameters() {
          ArrayList<String> res = new ArrayList<String>();
          for( Argument a : request.arguments() )
            if( a._required ) res.add(a._name);
          return res.toArray(new String[0]);
        }

        @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
          for( Argument a : request.arguments() )
            a.reset();
          for( Argument a : request.arguments() ) {
            int pos = ai.position(a._name);
            if( pos >= 0 && !a.disabled() ) {
              try {
                a.check(Interop.asString(args[pos].asString()));
              } catch( IllegalArgumentException e ) {
                throw Log.errRTExcept(e);
              }
            }
          }
          return toR(Request.execSync(request));
        }
      });
    }
    Interop.register(new Kmeans());
    Interop.register(new RGLM());
    Interop.register(new Load());
    Interop.register(new Parse());
    Interop.register(new Save());
    Interop.register(new VARead());
    Interop.register(new VAWrite());
    Console.main(args);
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
      return RListFactory.getFor(a, new int[] { a.length }, Names.create(names));
    }
    if( e instanceof JsonArray ) {
      RAny[] a = new RAny[((JsonArray) e).size()];
      for( int i = 0; i < a.length; i++ )
        a[i] = toR(((JsonArray) e).get(i));
      return RListFactory.getFor(a);
    }
    return Interop.asRString(e.getAsString());
  }
}
