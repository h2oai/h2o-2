package water.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import water.Log;

/**
 * Executes code on a separate machine.
 */
public class SeparateBox implements Separate {
  private static final String TARGET = "deployed";
  private final String        _host, _user, _key;
  private final String[]      _args;
  private final Thread        _thread;

  public SeparateBox(String host, String user, String key, String[] args) {
    _host = host;
    _user = user;
    _key = key;
    _args = args;

    _thread = new Thread() {
      @Override
      public void run() {
        try {
          deploy();

          String[] args = new String[] { _host, _user, _key };
          if( _args != null )
            args = (String[]) ArrayUtils.addAll(args, _args);
          SSH ssh = new SSH(args);
          ssh.waitForEnd();
        } catch( Exception e ) {
          Log.write(e);
        }
      }
    };

    _thread.setDaemon(true);
    _thread.start();
  }

  @Override
  public void close() {
    close(_host, _user, _key);
  }

  static void close(String host, String user, String key) {
    try {
      ArrayList<String> list = new ArrayList<String>();
      list.addAll(Arrays.asList(ssh(user, key).split(" ")));
      list.add(host);
      list.add("pkill -u " + user + " java");

      ProcessBuilder builder = new ProcessBuilder(list);
      builder.environment().put("CYGWIN", "nodosfilewarning");
      builder.redirectErrorStream(true);
      Process process = builder.start();
      process.waitFor();
    } catch( Exception e ) {
      Log.write(e);
    }
  }

  @Override
  public void waitForEnd() {
    try {
      _thread.join();
    } catch( InterruptedException e ) {
    }
  }

  private static String ssh(String user, String key) {
    String ssh = "ssh";
    File onWindows = new File("C:/cygwin/bin/ssh.exe");
    if( onWindows.exists() ) {
      // Permissions are not always set correctly
      // TODO automate:
      // cd .ssh
      // chgrp Users id_rsa
      // chmod 600 id_rsa
      ssh = onWindows.getAbsolutePath();
    }
    return ssh + " -l " + user + " -i " + key //
        + " -o UserKnownHostsFile=/dev/null" //
        + " -o StrictHostKeyChecking=no" //
        + " -o LogLevel=quiet";
  }

  // Deploy exploded for now, could also rsync as jar file
  public void deploy() throws Exception {
    ArrayList<String> args = new ArrayList<String>();
    File onWindows = new File("C:/cygwin/bin/rsync.exe");
    args.add(onWindows.exists() ? onWindows.getAbsolutePath() : "rsync");
    args.add("-vrzute");
    args.add(ssh(_user, _key));
    args.add("--delete");
    args.add("--chmod=u=rwx");

    args.add("--exclude");
    args.add("'build/*.jar'");
    args.add("--exclude");
    args.add("'lib/hexbase_impl.jar'");
    args.add("--exclude");
    args.add("'lib/javassist'");

    ArrayList<String> sources = new ArrayList<String>();
    sources.add("build");
    sources.add("lib");
    for( int i = 0; i < sources.size(); i++ ) {
      String path = new File(sources.get(i)).getAbsolutePath();
      // Adapts paths in case running on Windows
      sources.set(i, path.replace('\\', '/').replace("C:/", "/cygdrive/c/"));
    }
    args.addAll(sources);

    args.add(_host + ":" + "/home/" + _user + "/" + TARGET);
    ProcessBuilder builder = new ProcessBuilder(args);
    builder.environment().put("CYGWIN", "nodosfilewarning");
    builder.redirectErrorStream(true);
    Process process = null;

    try {
      process = builder.start();
      SeparateVM.inheritIO(process, "rsync to " + _host, true);
      process.waitFor();
    } finally {
      if( process != null ) {
        try {
          process.destroy();
        } catch( Exception _ ) {
        }
      }
    }
  }

  /**
   * Invokes ssh from a separate VM which only job is to wait for its parent to
   * be gone, then kill all remote java processes for this user. Otherwise every
   * killed test leaves a bunch of orphan ssh and java instances.
   */
  static class SSH extends SeparateVM {
    public SSH(String[] args) throws IOException {
      super(null, args);
    }

    public static void main(String[] args) throws Exception {
      final String host = args[0], user = args[1], key = args[2];

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          SeparateBox.close(host, user, key);
        }
      });

      exitWithParent();

      ArrayList<String> list = new ArrayList<String>();
      list.addAll(Arrays.asList(ssh(user, key).split(" ")));
      list.add(host);

      // Port forwarding
      // args.add("-L");
      // args.add("8000:127.0.0.1:" + local);

      String debug = "", cp = "";
      // TODO switch to address=127.0.0.1:8000 & port forwarding for security
      // debug =
      // "-agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=n";
      int shared = new File(".").getCanonicalPath().length() + 1;
      for( String s : System.getProperty("java.class.path").split(File.pathSeparator) ) {
        cp += cp.length() != 0 ? ":" : "";
        cp += new File(s).getCanonicalPath().substring(shared).replace('\\', '/');
      }

      String command = "" //
          + "cd " + TARGET + ";" //
          + "java " + debug + " -ea -cp " + cp;
      for( int i = 3; i < args.length; i++ )
        command += " " + args[i];
      list.add(command);

      ProcessBuilder builder = new ProcessBuilder(list);
      builder.environment().put("CYGWIN", "nodosfilewarning");
      builder.redirectErrorStream(true);
      Process process = builder.start();
      SeparateVM.inheritIO(process, null, false);
      process.waitFor();
    }
  }
}
