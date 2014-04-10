package water.deploy;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import water.Boot;
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
    _args.addAll(Arrays.asList(java));
    if( args != null )
      _args.addAll(Arrays.asList(args));
  }

  static String[] javaArgs(String main) {
    RuntimeMXBean r = ManagementFactory.getRuntimeMXBean();
    ArrayList<String> list = new ArrayList<String>();
    for( String s : r.getInputArguments() )
      if( !s.startsWith("-agentlib") )
        if( !s.startsWith("-Xbootclasspath") )
          list.add(s);
    if( System.getProperty(H2O.DEBUG_ARG) != null )
      if( list.indexOf("-D" + H2O.DEBUG_ARG) < 0 )
        list.add("-D" + H2O.DEBUG_ARG);
    list.add(main);
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
      if( _inherit )
        inheritIO(_process, null);
      if( _out != null )
        persistIO(_process, _out, _err);
    } catch( IOException e ) {
      throw Log.errRTExcept(e);
    }
  }

  public boolean isAlive() {
    try {
      _process.exitValue();
      return false;
    } catch( IllegalThreadStateException xe ) {
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
    } catch( InterruptedException xe ) {
      // Ignore
    }
  }

  public static void exitWithParent() {
    Thread thread = new Thread() {
      @Override public void run() {
        // Avoid on Windows as it exits immediately. Seems to work using Java7
        // ProcessBuilder.redirectInput, but we need to run on Java 6 for now
        if( !System.getProperty("os.name").toLowerCase().contains("win") ) {
          for( ;; ) {
            int b;
            try {
              b = System.in.read();
            } catch( Exception e ) {
              b = -1;
            }
            if( b < 0 ) {
              Log.info("Assuming parent done, exit(0)");
              H2O.exit(0);
            }
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
            if( line == null )
              break;
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

  /**
   * A VM whose only job is to wait for its parent to be gone, then kill its child process.
   * Otherwise every killed test leaves a bunch of orphan ssh and java processes.
   */
  public static class Watchdog extends VM {
    public Watchdog(String[] java, String[] node) {
      super(java, node);
    }

    protected static void exec(ArrayList<String> list) throws Exception {
      ProcessBuilder builder = new ProcessBuilder(list);
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

    Params(Host host, String[] java, String[] node) {
      _host = new String[] { host.address(), host.user(), host.key() };
      _java = java;
      _node = node;
    }
  }

  public static File h2oFolder() {
    File target;
    if( Boot._init.fromJar() )
      target = new File(Boot._init.jarPath());
    else {
      try {
        URL url = Boot._init.getResource(H2O.class.getName().replace('.', '/') + ".class");
        target = new File(url.toURI()).getParentFile().getParentFile().getParentFile();
      } catch( URISyntaxException e ) {
        throw new RuntimeException(e);
      }
    }
    return target.getParentFile();
  }

  /**
   * A remote JVM, launched over SSH.
   */
  public static class SSH extends Watchdog {
    Host _host;
    Thread _thread;

    public SSH(Host host, String[] java, String[] node) {
      this(new String[] { SSH.class.getName() }, host, java, node);
    }

    public SSH(String[] localJava, Host host, String[] java, String[] node) {
      super(localJava, new String[] { write(new Params(host, java, node)) });
      _host = host;
    }

    public Host host() {
      return _host;
    }

    final void startThread() {
      _thread = new Thread() {
        @Override public void run() {
          try {
            SSH.this.start();
            SSH.this.waitFor();
          } catch( Exception ex ) {
            Log.err(ex);
          }
        }
      };
      _thread.setDaemon(true);
      _thread.start();
    }

    public static void main(String[] args) throws Exception {
      exitWithParent();
      Params p = read(args[0]);
      Host host = new Host(p._host[0], p._host[1], p._host[2]);
      ArrayList<String> list = new ArrayList<String>();
      list.addAll(Arrays.asList(host.sshWithArgs().split(" ")));
      list.add(host.address());
      list.add(command(p._java, p._node));
      exec(list);
    }

    static String command(String[] javaArgs, String[] nodeArgs) {
      String cp = "";
      try {
        String h2o = h2oFolder().getCanonicalPath();
        for( String s : System.getProperty("java.class.path").split(File.pathSeparator) ) {
          cp += cp.length() != 0 ? ":" : "";
          String path = new File(s).getCanonicalPath();
          if( path.startsWith(h2o) )
            path = path.substring(h2o.length() + 1);
          cp += path.replace('\\', '/').replace(" ", "\\ ");
        }
      } catch( IOException e ) {
        throw Log.errRTExcept(e);
      }
      String java = Cloud.JRE != null ? new File(Cloud.JRE).getName() + "/bin/java" : "java";
      String command = "cd " + Host.FOLDER + ";" + java + " -cp " + cp;
      for( String s : javaArgs )
        command += " " + s;
      for( String s : nodeArgs )
        command += " " + s;
      return command.replace("$", "\\$");
    }
  }

  static String write(Serializable s) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutput out = null;
      try {
        out = new ObjectOutputStream(bos);
        out.writeObject(s);
        return StringUtils.newStringUtf8(Base64.encodeBase64(bos.toByteArray(), false));
      } finally {
        out.close();
        bos.close();
      }
    } catch( Exception ex ) {
      throw Log.errRTExcept(ex);
    }
  }

  static <T> T read(String s) {
    try {
      ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(s)));
      try {
        return (T) in.readObject();
      } finally {
        in.close();
      }
    } catch( Exception ex ) {
      throw Log.errRTExcept(ex);
    }
  }
}
