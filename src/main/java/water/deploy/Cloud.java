package water.deploy;

import java.io.File;
import java.util.*;

import water.*;
import water.H2O.FlatFileEntry;
import water.H2O.OptArgs;
import water.deploy.VM.Watchdog;
import water.util.Log;
import water.util.Utils;

public class Cloud {
  private final String[] _publicIPs, _privateIPs;

  public Cloud(String[] publicIPs, String[] privateIPs) {
    _publicIPs = publicIPs;
    _privateIPs = privateIPs;
  }

  public String[] publicIPs() {
    return _publicIPs;
  }

  public String[] privateIPs() {
    return _privateIPs;
  }

  public void start(String[] includes, String[] excludes, String[] java_args, String[] args) {
    // Take first box as cloud master
    Host master = new Host(_publicIPs[0]);
    Set<String> incls = Host.defaultIncludes();
    Set<String> excls = Host.defaultExcludes();
    incls.addAll(Arrays.asList(includes));
    excls.addAll(Arrays.asList(excludes));
    File flatfile = Utils.writeFile(Utils.join('\n', _privateIPs));
    incls.add(flatfile.getAbsolutePath());
    master.rsync(incls, excls, false);

    ArrayList<String> list = new ArrayList<String>();
    list.add("-mainClass");
    list.add(Master.class.getName());
    list.add("-flatfile");
    list.add(flatfile.getName());
    list.addAll(Arrays.asList(args));
    String[] java = Utils.add(java_args, NodeVM.class.getName());
    SSHWatchdog r = new SSHWatchdog(master, java, list.toArray(new String[0]));
    r.inheritIO();
    r.start();
  }

  static class SSHWatchdog extends Watchdog {
    public SSHWatchdog(Host host, String[] java, String[] node) {
      super(null, new String[] { new Params(host, java, node).write() });
    }

    public static void main(String[] args) throws Exception {
      exitWithParent();

      Params p = Params.read(args[0]);
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

  static class Master {
    public static void main(String[] args) throws Exception {
      VM.exitWithParent();

      Arguments arguments = new Arguments(args);
      OptArgs h2oArgs = new OptArgs();
      arguments.extract(h2oArgs);
      String[] workerArgs = new String[] { "-flatfile", h2oArgs.flatfile };

      List<FlatFileEntry> flatfile = H2O.parseFlatFile(new File(h2oArgs.flatfile));
      HashMap<String, Host> hosts = new HashMap<String, Host>();
      ArrayList<Node> workers = new ArrayList<Node>();
      for( int i = 1; i < flatfile.size(); i++ ) {
        Host host = new Host(flatfile.get(i).inet.getHostAddress());
        hosts.put(host.address(), host);
        workers.add(new NodeHost(host, workerArgs));
      }

      Set<String> includes = Host.defaultIncludes();
      includes.add(h2oArgs.flatfile);
      Set<String> excludes = Host.defaultExcludes();
      Host.rsync(hosts.values().toArray(new Host[0]), includes, excludes, false);

      for( Node w : workers ) {
        w.inheritIO();
        w.start();
      }
      Boot.main(args);

      TestUtil.stall_till_cloudsize(1 + workers.size());
      Thread.sleep(1000);
      Log.unwrap(System.out, "");
      Log.unwrap(System.out, "The cloud is running. Connect through local port forwarding:");
      Log.unwrap(System.out, "http://127.0.0.1:54321");
    }
  }
}