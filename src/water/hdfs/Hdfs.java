package water.hdfs;

import java.io.File;

import water.H2O;
import water.Log;
import H2OInit.Boot;

public class Hdfs {
  private static final String DEFAULT_HDFS_VERSION = "cdh4";
  private static final String MAPRFS_HDFS_VERSION = "0.20.2";

  public static boolean initialize() {
    assert (H2O.OPT_ARGS.hdfs != null);
    if (H2O.OPT_ARGS.hdfs.equals("resurrect")) {
      throw new Error("HDFS resurrection is unimplemented");
    } else {
      // Load the HDFS backend for existing hadoop installations.
      // understands -hdfs=hdfs://server:port OR -hdfs=maprfs:///mapr/node_name/volume
      //             -hdfs-root=root
      //             -hdfs-config=config file
      String version = H2O.OPT_ARGS.hdfs_version==null ? DEFAULT_HDFS_VERSION : H2O.OPT_ARGS.hdfs_version;
      // If HDFS URI is MapR-fs - Switch two MapR version of hadoop
      version = version.equals("mapr") || (H2O.OPT_ARGS.hdfs.startsWith("maprfs://")) ? MAPRFS_HDFS_VERSION : version;
      try {
        if( Boot._init.fromJar() ) {
          File f = new File(version);
          if( f.exists() ) {
            Boot._init.addExternalJars(f);
          } else {
            Boot._init.addInternalJars("hadoop/"+version+"/");
          }
        }
      } catch(Exception e) {
        e.printStackTrace();
        Log.die("[hdfs] Unable to initialize hadoop version " + version +
            " please use different version.");
        return false;
      }
      PersistHdfs.ROOT.toString(); // Touch & thus start HDFS
      return true;
    }
  }
}
