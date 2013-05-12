package water.deploy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import water.Boot;
import water.deploy.VM.Watchdog;
import water.util.Log;

/**
 * Creates a node on a host.
 */
public class NodeHost implements Node {
  private volatile SSH _ssh;

  public NodeHost(Host host, String[] args) {
    _ssh = new SSH(host, args);
  }

  public Host host() {
    return _ssh.host();
  }

  @Override public void inheritIO() {
    _ssh.inheritIO();
  }

  @Override public void persistIO(String outFile, String errFile) throws IOException {
    _ssh.persistIO(outFile, errFile);
  }

  @Override public void start() {
    _ssh.startThread();
  }

  @Override public int waitFor() {
    try {
      _ssh._thread.join();
    } catch( InterruptedException e ) {}
    return 0;
  }

  @Override public void kill() {
    try {
      _ssh.kill();
    } catch( Exception _ ) {}
  }

  public static String command(String[] javaArgs, String[] nodeArgs) {
    ArrayList<String> list = new ArrayList<String>();
    if( javaArgs != null ) list.addAll(Arrays.asList(javaArgs));

    String cp = "";
    try {
      int shared = new File(".").getCanonicalPath().length() + 1;
      for( String s : System.getProperty("java.class.path").split(File.pathSeparator) ) {
        cp += cp.length() != 0 ? ":" : "";
        if( Boot._init.fromJar() ) cp += new File(s).getName();
        else cp += new File(s).getCanonicalPath().substring(shared).replace('\\', '/');
      }
      list.add("-cp");
      list.add(cp);
    } catch( IOException e ) {
      throw Log.errRTExcept(e);
    }

    String command = "cd " + Host.FOLDER + ";java";
    for( String s : list )
      command += " " + s;
    command += " " + NodeVM.class.getName();
    for( String s : nodeArgs )
      command += " " + s;
    return command.replace("$", "\\$");
  }

  static class SSH extends Watchdog {
    Host _host;
    Thread _thread;

    public SSH(Host host, String[] args) {
      super(null, new String[] { new Params(host, VM.cloneParams(), args).write() });
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
      Params p = Params.read(args[0]);
      Host host = new Host(p._host[0], p._host[1], p._host[2]);
      ArrayList<String> list = new ArrayList<String>();
      list.addAll(Arrays.asList(host.sshWithArgs().split(" ")));
      list.add(host.address());
      list.add(command(p._java, p._node));
      exec(list);
    }
  }
}
