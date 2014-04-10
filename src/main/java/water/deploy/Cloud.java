package water.deploy;

import java.io.File;
import java.io.Serializable;
import java.util.*;

import water.*;
import water.H2O.FlatFileEntry;
import water.deploy.VM.Params;
import water.deploy.VM.Watchdog;
import water.util.Log;
import water.util.Utils;

/**
 * Deploys and starts a remote cluster.
 * <br>
 * Note: This class is intended for debug and experimentation purposes only, please refer to the
 * documentation to run an H2O cluster.
 */
public class Cloud {
  public final List<String> publicIPs = new ArrayList<String>();
  public final List<String> privateIPs = new ArrayList<String>();
  /** Includes for rsync to the master */
  public final Set<String> clientRSyncIncludes = new HashSet<String>();
  /** Excludes for rsync to the master */
  public final Set<String> clientRSyncExcludes = new HashSet<String>();
  /** Includes for rsync between the master and slaves */
  public final Set<String> fannedRSyncIncludes = new HashSet<String>();
  /** Excludes for rsync between the master and slaves */
  public final Set<String> fannedRSyncExcludes = new HashSet<String>();

  /** Port for all remote machines. */
  public static final int PORT = 54423;
  public static final int FORWARDED_LOCAL_PORT = 54321;
  /**
   * To avoid configuring remote machines, a JVM can be sent through rsync with H2O. By default,
   * decompress the Oracle Linux x64 JDK to a local folder and point this path to it.
   */
  static final String JRE = null; // System.getProperty("user.home") + "/libs/jdk/jre";
  /** Watch dogs are additional JVMs that shutdown the cluster when the client is killed */
  static final boolean WATCHDOGS = true;
  static final String FLATFILE = "flatfile";

  public void start(String[] java_args, String[] args) {
    // Take first box as cloud master
    Host master = new Host(publicIPs.get(0));
    Set<String> incls = new HashSet<String>(clientRSyncIncludes);
    if( JRE != null && !new File(JRE + "/bin/java").exists() )
      throw new IllegalArgumentException("Invalid JRE");
    if( JRE != null )
      incls.add(JRE);
    List<String> ips = privateIPs.size() > 0 ? privateIPs : publicIPs;
    String s = "";
    for( Object o : ips )
      s += (s.length() == 0 ? "" : '\n') + o.toString() + ":" + PORT;
    File flatfile = Utils.writeFile(new File(Utils.tmp(), FLATFILE), s);
    incls.add(flatfile.getAbsolutePath());
    master.rsync(incls, clientRSyncExcludes, false);

    ArrayList<String> list = new ArrayList<String>();
    list.add("-mainClass");
    list.add(Master.class.getName());
    CloudParams p = new CloudParams();
    p._incls = new HashSet<String>(fannedRSyncIncludes);
    p._excls = fannedRSyncExcludes;
    p._incls.add(FLATFILE);
    if( JRE != null )
      p._incls.add(new File(JRE).getName());
    list.add(VM.write(p));
    list.addAll(Arrays.asList(args));
    String[] java = Utils.append(java_args, NodeVM.class.getName());
    Params params = new Params(master, java, list.toArray(new String[0]));
    if( WATCHDOGS ) {
      SSHWatchdog r = new SSHWatchdog(params);
      r.inheritIO();
      r.start();
    } else {
      try {
        SSHWatchdog.run(params);
      } catch( Exception e ) {
        throw new RuntimeException(e);
      }
    }
  }

  static class CloudParams implements Serializable {
    Set<String> _incls, _excls;
  }

  static class SSHWatchdog extends Watchdog {
    public SSHWatchdog(Params p) {
      super(javaArgs(SSHWatchdog.class.getName()), new String[] { write(p) });
    }

    public static void main(String[] args) throws Exception {
      exitWithParent();
      Params p = read(args[0]);
      run(p);
    }

    static void run(Params p) throws Exception {
      Host host = new Host(p._host[0], p._host[1], p._host[2]);
      String key = host.key() != null ? host.key() : "";
      String s = "ssh-agent sh -c \"ssh-add " + key + "; ssh -l " + host.user() + " -A" + Host.SSH_OPTS;
      s += " -L " + FORWARDED_LOCAL_PORT + ":127.0.0.1:" + PORT; // Port forwarding
      s += " " + host.address() + " '" + SSH.command(p._java, p._node) + "'\"";
      s = s.replace("\\", "\\\\").replace("$", "\\$");
      ArrayList<String> list = new ArrayList<String>();
      // Have to copy to file for cygwin, but works also on -nix
      File sh = Utils.writeFile(s);
      File onWindows = new File("C:/cygwin/bin/bash.exe");
      if( onWindows.exists() ) {
        list.add(onWindows.getPath());
        list.add("--login");
      } else
        list.add("bash");
      list.add(sh.getAbsolutePath());
      exec(list);
    }
  }

  public static class Master {
    public static void main(String[] args) throws Exception {
      VM.exitWithParent();

      CloudParams params = VM.read(args[0]);
      args = Utils.remove(args, 0);
      String[] workerArgs = new String[] { "-flatfile", FLATFILE, "-port", "" + PORT };

      List<FlatFileEntry> flatfile = H2O.parseFlatFile(new File(FLATFILE));
      HashMap<String, Host> hosts = new HashMap<String, Host>();
      ArrayList<Node> workers = new ArrayList<Node>();
      for( int i = 1; i < flatfile.size(); i++ ) {
        Host host = new Host(flatfile.get(i).inet.getHostAddress());
        hosts.put(host.address(), host);
        workers.add(new NodeHost(host, workerArgs));
      }
      Host.rsync(hosts.values().toArray(new Host[0]), params._incls, params._excls, false);

      for( Node w : workers ) {
        w.inheritIO();
        w.start();
      }
      H2O.main(Utils.append(workerArgs, args));
      stall_till_cloudsize(1 + workers.size(), 10000); // stall for cloud 10seconds
      Log.unwrap(System.out, "");
      Log.unwrap(System.out, "Cloud is up, local port " + FORWARDED_LOCAL_PORT + " forwarded");
      Log.unwrap(System.out, "Go to http://127.0.0.1:" + FORWARDED_LOCAL_PORT);
      Log.unwrap(System.out, "");
      int index = Arrays.asList(args).indexOf("-mainClass");
      if( index >= 0 ) {
        String pack = args[index + 1].substring(0, args[index + 1].lastIndexOf('.'));
        LaunchJar.weavePackages(pack);
        Boot.run(args);
      }
    }
    public static void stall_till_cloudsize(int x, long ms) {
      H2O.waitForCloudSize(x, ms);
      UKV.put(Job.LIST, new Job.List()); // Jobs.LIST must be part of initial keys
    }
  }
}
