package water.deploy;

import java.io.File;
import java.util.*;

import water.Boot;
import water.util.Log;
import water.util.Utils;

public class Host {
  public static final String SSH_OPTS;

  static {
    SSH_OPTS = "" //
        + " -o UserKnownHostsFile=/dev/null" //
        + " -o StrictHostKeyChecking=no" //
        + " -o LogLevel=quiet" //
        + " -o ServerAliveInterval=15" //
        + " -o ServerAliveCountMax=3";
  }

  public static final String FOLDER = "h2o_rsync";
  private final String _address, _user, _key;

  public Host(String addr) {
    this(addr, null);
  }

  public Host(String addr, String user) {
    this(addr, user, null);
  }

  public Host(String addr, String user, String key) {
    _address = addr;
    _user = user != null ? user : System.getProperty("user.name");
    _key = key;
  }

  public String address() {
    return _address;
  }

  public String user() {
    return _user;
  }

  public String key() {
    return _key;
  }

  public static Set<String> defaultIncludes() {
    Set<String> set = new HashSet<String>();
    if( Boot._init.fromJar() ) {
      if( new File("target/h2o.jar").exists() )
        set.add("target/h2o.jar");
      else
        set.add("h2o.jar");
    } else {
      set.add("target");
      set.add("lib");
    }
    return set;
  }

  public static Set<String> defaultExcludes() {
    Set<String> set = new HashSet<String>();
    if( !Boot._init.fromJar() ) {
      set.add("target/*.jar");
      set.add("target/javadoc/**");
      set.add("lib/javassist");
      set.add("**/*-sources.jar");
    }
    return set;
  }

  public void rsync() {
    rsync(defaultIncludes(), defaultExcludes(), false);
  }

  public void rsync(Set<String> includes, Set<String> excludes, boolean delete) {
    Process process = null;
    try {
      ArrayList<String> args = new ArrayList<String>();
      args.add("rsync");
      args.add("-vrzute");
      args.add(sshWithArgs());
      args.add("--chmod=u=rwx");

      for( String s : includes )
        args.add(new File(s).getCanonicalPath());

      // --exclude seems ignored on Linux (?) so use --exclude-from
      File file = Utils.writeFile(Utils.join('\n', excludes));
      args.add("--exclude-from");
      args.add(file.getCanonicalPath());
      if( delete )
        args.add("--delete");

      args.add(_address + ":" + "/home/" + _user + "/" + FOLDER);
      ProcessBuilder builder = new ProcessBuilder(args);
      process = builder.start();
      String log = "rsync " + VM.localIP() + " -> " + _address;
      NodeVM.inheritIO(process, Log.padRight(log + ": ", 24));
      process.waitFor();
    } catch( Exception ex ) {
      throw new RuntimeException(ex);
    } finally {
      if( process != null ) {
        try {
          process.destroy();
        } catch( Exception _ ) {
          // Ignore
        }
      }
    }
  }

  public static void rsync(final Host... hosts) {
    rsync(hosts, defaultIncludes(), defaultExcludes(), false);
  }

  public static void rsync(final Host[] hosts, final Set<String> includes, final Set<String> excludes,
      final boolean delete) {
    ArrayList<Thread> threads = new ArrayList<Thread>();

    for( int i = 0; i < hosts.length; i++ ) {
      final int i_ = i;
      Thread t = new Thread() {
        @Override public void run() {
          hosts[i_].rsync(includes, excludes, delete);
        }
      };
      t.setDaemon(true);
      t.start();
      threads.add(t);
    }

    for( Thread t : threads ) {
      try {
        t.join();
      } catch( InterruptedException e ) {
        throw Log.errRTExcept(e);
      }
    }
  }

  String sshWithArgs() {
    String k = "";
    if( _key != null ) {
      assert new File(_key).exists();
      // Git doesn't set permissions, so force them each time
      try {
        Process p = Runtime.getRuntime().exec("chmod 600 " + _key);
        p.waitFor();
      } catch( Exception e ) {
        throw Log.errRTExcept(e);
      }
      k = " -i " + _key;
    }
    return "ssh -l " + _user + " -A" + k + SSH_OPTS;
  }
}
