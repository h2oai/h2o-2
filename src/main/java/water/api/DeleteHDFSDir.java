package water.api;

import static water.util.FSUtils.isHdfs;
import static water.util.FSUtils.isS3N;

import java.io.File;
import java.io.IOException;

import hex.glm.GLMModel;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import water.*;
import water.persist.PersistHdfs;
import water.serial.Model2FileBinarySerializer;
import water.serial.Model2HDFSBinarySerializer;
import water.util.FSUtils;



/**
 * Created by amy on 12/5/14.
 */
public class DeleteHDFSDir extends Func{
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Path of directory in HDFS to be removed.", required = true, filter = Default.class, json=true, gridable = false)
  String path;

  @Override
  protected void execImpl() {
    if (isHdfs(path)) deleteHDFSDir();
  }

  private void deleteHDFSDir() {
    if (FSUtils.isBareS3NBucketWithoutTrailingSlash(path)) { path += "/"; }
    Path parentDir = new Path(path);
    try {
      FileSystem fs = FileSystem.get(parentDir.toUri(), PersistHdfs.CONF);
      fs.delete(parentDir);
    } catch( IOException e ) {
      throw new IllegalArgumentException("Cannot delete file " + path, e);
    }
  }


}
