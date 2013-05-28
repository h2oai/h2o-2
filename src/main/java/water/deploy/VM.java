package water.deploy;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;

import water.H2O;
import water.util.Log;

/**
 * Executes code in a separate VM.
 */
public abstract class VM {
  private final ArrayList<String> _args;
  private Process _process;
  private boolean _inherit;
  private File _out, _err;

  public VM(String[] java, String[] args) {
    _args = new ArrayList<String>();
    _args.add(System.getProperty("java.home") + "/bin/java");
    if( java != null ) _args.addAll(Arrays.asList(java));

    // Iterate on URIs in case jar has been unpacked by Boot
    _args.add("-cp");
    String cp = "";
    for( URL url : ((URLClassLoader) ClassLoader.getSystemClassLoader()).getURLs() ) {
      try {
        cp += new File(new URI(url.toString())) + File.pathSeparator;
      } catch( URISyntaxException e ) {
        throw Log.errRTExcept(e);
      }
    }
    _args.add(cp);
    _args.add(getClass().getName());
    if( args != null ) _args.addAll(Arrays.asList(args));
  }

  static String[] cloneParams() {
    RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();
    ArrayList<String> list = new ArrayList<String>();
    for( String s : r.getInputArguments() )
      if( !s.startsWith("-agentlib") ) list.add(s);
    return list.toArray(new String[list.size()]);
  }

  public Process process() {
    return _process;
  }

  public void inheritIO() {
    _inherit = true;
  }

  public void persistIO(String out, String err) throws IOException {
    _out = new File(out);
    _err = new File(err);
  }

  public void start() {
    ProcessBuilder builder = new ProcessBuilder(_args);
    try {
      assert !_inherit || (_out == null);
      _process = builder.start();
      if( _inherit ) inheritIO(_process, null);
      if( _out != null ) persistIO(_process, _out, _err);
    } catch( IOException e ) {
      throw Log.errRTExcept(e);
    }
  }

  public boolean isAlive() {
    try {
      _process.exitValue();
      return false;
    } catch( IllegalThreadStateException _ ) {
      return true;
    } catch( Exception e ) {
      throw Log.errRTExcept(e);
    }
  }

  public int waitFor() {
    try {
      return _process.waitFor();
    } catch( InterruptedException e ) {
      throw Log.errRTExcept(e);
    }
  }

  public void kill() {
    _process.destroy();
    try {
      _process.waitFor();
    } catch( InterruptedException _ ) {
      // Ignore
    }
  }

  public static void exitWithParent() {
    Thread thread = new Thread() {
      @Override public void run() {
        for( ;; ) {
          int b;
          try {
            b = System.in.read();
          } catch( Exception e ) {
            b = -1;
          }
          if( b < 0 ) {
            Log.debug("Assuming parent done, exit(0)");
            System.exit(0);
          }
        }
      }
    };
    thread.setDaemon(true);
    thread.start();
  }

  public static void inheritIO(Process process, final String header) {
    forward(process, header, process.getInputStream(), System.out);
    forward(process, header, process.getErrorStream(), System.err);
  }

  public static void persistIO(Process process, File out, File err) throws IOException {
    forward(process, null, process.getInputStream(), new PrintStream(out));
    forward(process, null, process.getErrorStream(), new PrintStream(err));
  }

  private static void forward(Process process, final String header, InputStream source, final PrintStream target) {
    final BufferedReader source_ = new BufferedReader(new InputStreamReader(source));
    Thread thread = new Thread() {
      @Override public void run() {
        try {
          for( ;; ) {
            String line = source_.readLine();
            if( line == null ) break;
            String s = header == null ? line : header + line;
            Log.unwrap(target, s);
          }
        } catch( IOException e ) {
          // Ignore, process probably done
        }
      }
    };
    thread.start();
  }

  public static String localIP() {
    return H2O.SELF_ADDRESS.getHostAddress();
  }

  /**
   * A VM whose only job is to wait for its parent to be gone, then kill its child process.
   * Otherwise every killed test leaves a bunch of orphan ssh and java processes.
   */
  static class Watchdog extends VM {
    public Watchdog(String[] java, String[] node) {
      super(java, node);
    }

    protected static void exec(ArrayList<String> list) throws Exception {
      ProcessBuilder builder = new ProcessBuilder(list);
      builder.environment().put("CYGWIN", "nodosfilewarning");
      final Process process = builder.start();
      NodeVM.inheritIO(process, null);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override public void run() {
          process.destroy();
        }
      });
      process.waitFor();
    }
  }

  static class Params implements Serializable {
    String[] _host, _java, _node;

    public Params(Host host, String[] java, String[] node) {
      _host = new String[] { host.address(), host.user(), host.key() };
      _java = java;
      _node = node;
    }

    String write() {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
          out = new ObjectOutputStream(bos);
          out.writeObject(this);
          return Base64.encodeBase64String(bos.toByteArray());
        } finally {
          out.close();
          bos.close();
        }
      } catch( Exception ex ) {
        throw Log.errRTExcept(ex);
      }
    }

    static Params read(String s) {
      try {
        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(s)));
        try {
          return (Params) in.readObject();
        } finally {
          in.close();
        }
      } catch( Exception ex ) {
        throw Log.errRTExcept(ex);
      }
    }
  }
}