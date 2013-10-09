package water.deploy;

import java.io.File;
import java.io.Serializable;
import java.util.*;

import water.*;
import water.H2O.FlatFileEntry;
import water.deploy.VM.Watchdog;
import water.util.Log;
import water.util.Utils;

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
  /**
   * To avoid configuring remote machines, the JDK is sent through rsync with H2O. By default,
   * decompress the Oracle Linux x64 JDK to a local folder and point this path to it.
   */
  public String jdk;

  public void start(String[] java_args, String[] args) {
    // Take first box as cloud master
    Host master = new Host(publicIPs.get(0));
    Set<String> incls = Host.defaultIncludes();
    Set<String> excls = Host.defaultExcludes();
    incls.addAll(clientRSyncIncludes);
    excls.addAll(clientRSyncExcludes);
    if( !new File(jdk + "/jre/bin/java").exists() )
      throw new IllegalArgumentException("Please specify the JDK to rsync and run on");
    incls.add(jdk);
    File flatfile;
    if( privateIPs.size() > 0 )
      flatfile = Utils.writeFile(Utils.join('\n', privateIPs));
    else
      flatfile = Utils.writeFile(Utils.join('\n', publicIPs));
    incls.add(flatfile.getAbsolutePath());
    master.rsync(incls, excls, false);

    ArrayList<String> list = new ArrayList<String>();
    list.add("-mainClass");
    list.add(Master.class.getName());
    CloudParams p = new CloudParams();
    p._incls = Host.defaultIncludes();
    p._excls = Host.defaultExcludes();
    p._incls.addAll(fannedRSyncIncludes);
    p._excls.addAll(fannedRSyncExcludes);
    p._incls.add(flatfile.getName());
    p._flatfile = flatfile.getName();
    p._incls.add(new File(jdk).getName());
    list.add(VM.write(p));
    list.addAll(Arrays.asList(args));
    String[] java = Utils.add(java_args, NodeVM.class.getName());
    SSHWatchdog r = new SSHWatchdog(master, java, list.toArray(new String[0]));
    r.inheritIO();
    r.start();
  }

  static class CloudParams implements Serializable {
    Set<String> _incls, _excls;
    String _flatfile;
  }

  static class SSHWatchdog extends Watchdog {
    public SSHWatchdog(Host host, String[] java, String[] node) {
      super(javaArgs(SSHWatchdog.class.getName()), new String[] { write(new Params(host, java, node)) });
    }

    public static void main(String[] args) throws Exception {
      exitWithParent();

      Params p = read(args[0]);
      Host host = new Host(p._host[0], p._host[1], p._host[2]);
      String key = host.key() != null ? host.key() : "";
      String s = "ssh-agent sh -c \"ssh-add " + key + "; ssh -l " + host.user() + " -A" + Host.SSH_OPTS;
      s += " -L 54321:127.0.0.1:54321"; // Port forwarding
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
      String[] workerArgs = new String[] { "-flatfile", params._flatfile };

      List<FlatFileEntry> flatfile = H2O.parseFlatFile(new File(params._flatfile));
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
      ArrayList<String> list = new ArrayList<String>(Arrays.asList(args));
      list.remove(0);
      list.addAll(Arrays.asList(workerArgs));
      args = list.toArray(new String[0]);
      H2O.main(args);
      if( list.indexOf(Boot.MAIN_CLASS) >= 0 ) {
        TestUtil.stall_till_cloudsize(1 + workers.size());
        Boot.run(args);
      } else {
        Thread.sleep(1000);
        Log.unwrap(System.out, "");
        Log.unwrap(System.out, "The cloud is running, with a port forwarded to:");
        Log.unwrap(System.out, "http://127.0.0.1:54321");
      }
    }
  }
}