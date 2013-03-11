package water.hdfs;

import java.io.File;

import water.*;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

public class HdfsLoader {
  private static final String DEFAULT_HDFS_VERSION = "cdh4";
  private static final String MAPRFS_HDFS_VERSION = "0.20.2mapr";

  public static void initialize() {
    // Load the HDFS backend for existing hadoop installations.
    // understands -hdfs=hdfs://server:port OR -hdfs=maprfs:///mapr/node_name/volume
    //             -hdfs-root=root
    //             -hdfs-config=config file
    String version = Objects.firstNonNull(H2O.OPT_ARGS.hdfs_version, DEFAULT_HDFS_VERSION);

    // If HDFS URI is MapR-fs - Switch two MapR version of hadoop
    if( "mapr".equals(version) ||
        Strings.nullToEmpty(H2O.OPT_ARGS.hdfs).startsWith("maprsfs://") ) {
      version = MAPRFS_HDFS_VERSION;
    }
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
    }
    PersistHdfs.initialize();
  }
}
