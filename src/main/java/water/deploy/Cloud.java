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
  public final List<String> _publicIPs = new ArrayList<String>();
  public final List<String> _privateIPs = new ArrayList<String>();
  public final Set<String> _clientRSyncIncludes = new HashSet<String>();
  public final Set<String> _clientRSyncExcludes = new HashSet<String>();
  public final Set<String> _fannedRSyncIncludes = new HashSet<String>();
  public final Set<String> _fannedRSyncExcludes = new HashSet<String>();

  public void start(String[] java_args, String[] args) {
    // Take first box as cloud master
    Host master = new Host(_publicIPs.get(0));
    Set<String> incls = Host.defaultIncludes();
    Set<String> excls = Host.defaultExcludes();
    incls.addAll(_clientRSyncIncludes);
    excls.addAll(_clientRSyncExcludes);
    File flatfile;
    if( _privateIPs.size() > 0 )
      flatfile = Utils.writeFile(Utils.join('\n', _privateIPs));
    else
      flatfile = Utils.writeFile(Utils.join('\n', _publicIPs));
    incls.add(flatfile.getAbsolutePath());
    master.rsync(incls, excls, false);

    ArrayList<String> list = new ArrayList<String>();
    list.add("-mainClass");
    list.add(Master.class.getName());
    CloudParams p = new CloudParams();
    p._incls = Host.defaultIncludes();
    p._excls = Host.defaultExcludes();
    p._incls.addAll(_fannedRSyncIncludes);
    p._excls.add(flatfile.getName());
    p._flatfile = flatfile.getName();
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
      Boot.main(Utils.remove(args, 0));

      waitForCloudSize(1 + workers.size(), 10000);
      Thread.sleep(1000);
      Log.unwrap(System.out, "");
      Log.unwrap(System.out, "The cloud is running, with a port forwarded to:");
      Log.unwrap(System.out, "http://127.0.0.1:54321");
    }

    static void waitForCloudSize(int size, int ms) {
      H2O.waitForCloudSize(size, ms);
      Job.putEmptyJobList();
    }
  }
}