package water.persist;

import java.io.File;

import water.Boot;
import water.H2O;
import water.util.Log;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

public class HdfsLoader {
  private static final String DEFAULT_HDFS_VERSION = "cdh4";
  private static final String MAPRFS_HDFS_VERSION = "mapr2.1.3";

  public static void loadJars() {
    if (H2O.OPT_ARGS.hdfs_skip != null) {
      // When H2O is launched by hadoop itself, it should use the HDFS library that
      // the hadoop mapper task picks up by default.
      //
      // Do not load any hadoop jar that is packed with H2O.
      Log.info("H2O was started by Hadoop; inheriting HDFS library from mapper task.");
      return;
    }

    if (H2O.OPT_ARGS.hdfs_version != null) {
      Log.info("HDFS version specified on the command line: " + H2O.OPT_ARGS.hdfs_version);
    }

    // Load the HDFS backend for existing hadoop installations.
    // FIX! hadoop/mapr supports other variants? also why isn't port an option on mapr, and why volume?
    // port should be optional
    // understands -hdfs=hdfs://server:port OR -hdfs=maprfs:///mapr/node_name/volume
    //             -hdfs-root=root
    //             -hdfs-config=config file
    String version = Objects.firstNonNull(H2O.OPT_ARGS.hdfs_version, DEFAULT_HDFS_VERSION);

    // If HDFS URI is MapR-fs - Switch to MapR version of hadoop
    // FIX! shouldn't we just use whatever the hdfs_version specifies previously?
    if( "mapr".equals(version) || Strings.nullToEmpty(H2O.OPT_ARGS.hdfs).startsWith("maprfs:///") ) {
      version = MAPRFS_HDFS_VERSION;
    }
    try {
      if( Boot._init.fromJar() ) {
        File f = new File(version);
        if( f.exists() ) {
          Boot._init.addExternalJars(f);
        } else {
          Boot._init.addInternalJars("hadoop/" + version + "/");
        }
      }
    } catch( Exception e ) {
      Log.err(e);
      Log.die("[hdfs] Unable to initialize hadoop version " + version + " please use different version.");
    }
  }
}
