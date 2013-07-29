package hex;

import java.io.File;
import java.util.ArrayList;
import java.util.Set;

import water.H2O;
import water.TestUtil;
import water.deploy.*;
import water.deploy.VM.SSH;
import water.util.Utils;

public class Mnist8mDist {
  public static void main(String[] args) throws Exception {
    water.Boot.main(UserMain.class, args);
  }

  public static class UserMain {
    public static void main(String[] args) throws Exception {
      String local = H2O.findInetAddressForSelf().getHostAddress();

      ArrayList<Host> list = new ArrayList<Host>();
      for( int i = 0; i < 10; i++ )
        list.add(new Host("192.168.1." + (171 + i)));
      for( int i = 0; i < 6; i++ )
        list.add(new Host("192.168.1.15" + i));
      Host[] workers = list.toArray(new Host[0]);

      int port = 54321;
      String flat = local + ":" + port + '\n';
      for( int i = 0; i < workers.length; i++ )
        flat += workers[i].address() + ":" + port + '\n';
      String flatfile = "target/flatfile";
      Utils.writeFile(new File(flatfile), flat);

      Set<String> includes = Host.defaultIncludes();
      Set<String> excludes = Host.defaultExcludes();
      Host.rsync(workers, includes, excludes, true);

      String[] shared = new String[] { "-port", "" + port, "-flatfile", flatfile };
      for( int i = 0; i < workers.length; i++ ) {
        String[] jdk = new String[] { "" + //
            "-D" + VM.JAVA_PATH + "=/usr/lib/jvm/java-7-oracle/jre/bin/java", SSH.class.getName() };
        String[] remot = new String[] { "-ea", "-Xmx12G", "-Dh2o.debug", NodeVM.class.getName() };
        SSH ssh = new SSH(jdk, workers[i], remot, shared);
        ssh.inheritIO();
        ssh.start();
      }
      H2O.main(shared);
      TestUtil.stall_till_cloudsize(1 + workers.length);
    }
  }
}