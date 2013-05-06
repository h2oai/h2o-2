package water.deploy;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import water.util.Log;

/**
 * Executes code in a separate VM.
 */
public abstract class VM {
  private final ArrayList<String> _args;
  private Process _process;
  private boolean _inherit;
  private File _out, _err;

  public VM(String[] args) {
    this(null, args);
  }

  public VM(String[] javaArgs, String[] appArgs) {
    _args = new ArrayList<String>();
    _args.add(System.getProperty("java.home") + "/bin/java");
    defaultParams(_args);

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

    if( javaArgs != null ) _args.addAll(Arrays.asList(javaArgs));
    _args.add(getClass().getName());
    if( appArgs != null ) _args.addAll(Arrays.asList(appArgs));
  }

  static void defaultParams(ArrayList<String> list) {
    boolean ea = false;
    assert ea = true;
    if( ea ) list.add("-ea");
    if( Host.LOG_RSYNC ) list.add("-D" + Host.LOG_RSYNC_NAME + "=true");
    // list.add("-agentlib:jdwp=transport=dt_socket,address=127.0.0.1:8001,server=y,suspend=n");
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
    } catch( InterruptedException _ ) {}
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
    return Log.HOST;
  }

  /**
   * A VM whose only job is to wait for its parent to be gone, then kill its child process.
   * Otherwise every killed test leaves a bunch of orphan ssh and java processes.
   */
  public static class Watchdog extends VM {
    private final Host _host;

    public Watchdog(Host host, String[] args) {
      super(addHost(host, args));
      _host = host;
    }

    public Host host() {
      return _host;
    }

    public static String[] addHost(Host host, String[] args) {
      String k = host.key() != null ? host.key() : "null";
      String[] res = new String[] { host.address(), host.user(), k };
      if( args != null ) res = (String[]) ArrayUtils.addAll(res, args);
      return res;
    }

    protected static Host getHost(String[] args) {
      String key = !args[2].equals("null") ? args[2] : null;
      return new Host(args[0], args[1], key);
    }

    protected static String[] getArgs(String[] args) {
      return Arrays.copyOfRange(args, 3, args.length);
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
}