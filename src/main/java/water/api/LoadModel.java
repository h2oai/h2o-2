package water.api;

import static water.util.FSUtils.isHdfs;
import static water.util.FSUtils.isS3N;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import water.Model;
import water.Request2;
import water.persist.PersistHdfs;
import water.serial.Model2FileBinarySerializer;
import water.serial.Model2HDFSBinarySerializer;

public class LoadModel extends Request2 {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Path to a file with saved model.", required = true, filter = Default.class, gridable = false)
  String path;

  @API(help = "Loaded model")
  Model model;

  @Override protected Response serve() {
    if (isHdfs(path) || isS3N(path)) loadFromHdfs();
    else loadFromLocalFS();
    return Inspector.redirect(this, model._key);
  }

  private void loadFromLocalFS() {
    File f = new File(path);
    if (!f.exists()) throw new IllegalArgumentException("File " +path+" does not exist!");
    try {
      model =new Model2FileBinarySerializer().load(f);
    } catch( IOException e ) {
      throw new IllegalArgumentException("Cannot load file " + path, e);
    }
  }

  private void loadFromHdfs() {
    Path f = new Path(path);
    try {
      FileSystem fs = FileSystem.get(f.toUri(), PersistHdfs.CONF);
      model = new Model2HDFSBinarySerializer(fs, false).load(f);
    } catch( IOException e ) {
      throw new IllegalArgumentException("Cannot load file " + path, e);
    }
  }
}
