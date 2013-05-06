package water.deploy;

import java.io.File;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import water.*;
import water.H2O.FlatFileEntry;
import water.H2O.OptArgs;
import water.util.Log;

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

  /**
   * Runs on one of the boxes.
   */
  public static class Master {
    public static void main(String[] args) throws Exception {
      VM.exitWithParent();

      Arguments arguments = new Arguments(args);
      OptArgs h2oArgs = new OptArgs();
      arguments.extract(h2oArgs);
      String[] workerArgs = new String[] { "-flatfile", h2oArgs.flatfile, "-log_headers" };

      List<FlatFileEntry> flatfile = H2O.parseFlatFile(new File(h2oArgs.flatfile));
      HashMap<String, Host> hosts = new HashMap<String, Host>();
      ArrayList<Node> workers = new ArrayList<Node>();
      for( int i = 1; i < flatfile.size(); i++ ) {
        Host host = new Host(flatfile.get(i).inet.getHostAddress());
        hosts.put(host.address(), host);
        workers.add(new NodeHost(host, null, workerArgs));
      }

      String[] includes = (String[]) ArrayUtils.add(Host.defaultIncludes(), h2oArgs.flatfile);
      String[] excludes = Host.defaultExcludes();
      Cloud.rsync(hosts.values().toArray(new Host[0]), includes, excludes);

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

  public static void rsync(final Host[] hosts, final String[] includes, final String[] excludes) {
    ArrayList<Thread> threads = new ArrayList<Thread>();

    for( int i = 0; i < hosts.length; i++ ) {
      final int i_ = i;
      Thread t = new Thread() {
        @Override public void run() {
          hosts[i_].rsync(includes, excludes);
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
}