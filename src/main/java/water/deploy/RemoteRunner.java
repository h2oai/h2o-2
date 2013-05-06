package water.deploy;

import java.io.File;
import java.util.ArrayList;

import water.deploy.VM.Watchdog;
import water.util.Utils;

public class RemoteRunner {
  public static VM launch(Host host, String[] args) {
    SSHWatchdog r = new SSHWatchdog(host, args);
    r.inheritIO();
    r.start();
    return r;
  }

  static class SSHWatchdog extends Watchdog {
    public SSHWatchdog(Host host, String[] args) {
      super(host, args);
    }

    public static void main(String[] args) throws Exception {
      exitWithParent();

      Host host = getHost(args);
      String key = host.key() != null ? host.key() : "";
      String s = "ssh-agent sh -c \"ssh-add " + key + "; ssh -l " + host.user() + " -A" + Host.SSH_OPTS;
      s += " -L 54321:127.0.0.1:54321"; // Port forwarding
      String[] log = Host.LOG_RSYNC ? new String[] { "-D" + Host.LOG_RSYNC_NAME + "=true" } : null;
      s += " " + host.address() + " '" + NodeHost.command(log, getArgs(args)) + "'\"";
      s = s.replace("\\", "\\\\").replace("$", "\\$");
      ArrayList<String> list = new ArrayList<String>();
      // Have to copy to file for cygwin, but works also on -nix
      File sh = Utils.tempFile(s);
      File onWindows = new File("C:/cygwin/bin/bash.exe");
      if( onWindows.exists() ) {
        list.add(onWindows.getPath());
        list.add("--login");
      } else list.add("bash");
      list.add(sh.getAbsolutePath());
      exec(list);
    }
  }
}
