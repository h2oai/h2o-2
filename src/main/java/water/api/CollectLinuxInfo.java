package water.api;

import water.*;
import water.api.RequestServer.API_VERSION;
import water.util.LinuxProcFileReader;
import water.util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Arrays;

public class CollectLinuxInfo extends Func {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text for GET.
  static final String DOC_GET = "Collect information available from Linux (does nothing for non-Linux)";

  static void collect() throws RuntimeException {
    // Sanity check that we're running on Linux.
    {
      LinuxProcFileReader lpfr = new LinuxProcFileReader();
      lpfr.read();
      if (!lpfr.valid()) {
        Log.info("CollectLinuxInfo couldn't collect anything because we're not running on Linux");
      }
    }

    // Clean up old directory, if one exists.
    String linuxInfoDirString = Log.getLogPathFileNameStem() + "-collect-linux-info";
    File linuxInfoDir = new File(linuxInfoDirString);
    if (linuxInfoDir.exists()) {
      try {
        String[] cmd = {"/bin/rm", "-r", "-f", linuxInfoDirString};
        Log.debug("Running command: " + Arrays.toString(cmd));
        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    if (linuxInfoDir.exists()) {
      throw new RuntimeException("Failed to delete directory " + linuxInfoDirString);
    }

    // Make new directory.
    try {
      String[] cmd = {"/bin/mkdir", "-p", linuxInfoDirString};
      Log.debug("Running command: " + Arrays.toString(cmd));
      Process p = Runtime.getRuntime().exec(cmd);
      p.waitFor();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (! linuxInfoDir.exists()) {
      throw new RuntimeException("Failed to create directory " + linuxInfoDirString);
    }

    try {
      String[] cmd = {"/bin/chmod", "u+rwx", linuxInfoDirString};
      Log.debug("Running command: " + Arrays.toString(cmd));
      Process p = Runtime.getRuntime().exec(cmd);
      p.waitFor();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Unpack the script.
    String collectFilename = "collect-linux-info.sh";
    InputStream is = Boot._init.getResource2("/diagnostics/" + collectFilename);
    File collectDirFilename = new File(linuxInfoDir, collectFilename);
    try {
      FileOutputStream os = new FileOutputStream(collectDirFilename);
      byte[] buffer = new byte[1024];
      int len = is.read(buffer);
      while (len != -1) {
        os.write(buffer, 0, len);
        len = is.read(buffer);
      }
      is.close();
      os.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Run the unpacked script.
    try {
      String[] cmd = {"/bin/sh", collectDirFilename.toString(), linuxInfoDirString};
      Log.debug("Running command: " + Arrays.toString(cmd));
      Process p = Runtime.getRuntime().exec(cmd);
      p.waitFor();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class CollectLinuxInfoTask extends DRemoteTask {
    @Override
    public void lcompute() {
      CollectLinuxInfo.collect();
      tryComplete();
    }

    @Override public void reduce(DRemoteTask drt) {}
  }

  @Override protected void execImpl() {
    CollectLinuxInfoTask task = new CollectLinuxInfoTask();
    task.invokeOnAllNodes();
  }

  @Override public API_VERSION[] supportedVersions() {
    return SUPPORTS_ONLY_V2;
  }
}
