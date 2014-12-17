package water.api;

import static water.util.FSUtils.isHdfs;
import static water.util.FSUtils.isS3N;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import water.Model;
import water.Request2;
import water.persist.PersistHdfs;
import water.serial.Model2FileBinarySerializer;
import water.serial.Model2HDFSBinarySerializer;
import water.util.FSUtils;

public class LoadModel extends Request2 {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Path of directory with saved model(s).", required = true, filter = Default.class, gridable = false)
  String path;

  @API(help = "Loaded model")
  Model model;

  @Override protected Response serve() {
    if (isHdfs(path) || isS3N(path)) loadFromHdfs();
    else loadFromLocalFS();
    return Inspector.redirect(this, model._key);
  }

  private void loadFromLocalFS() {
    File parentDir = new File(path);
    File model_names = new File(parentDir, "model_names");
    if (!parentDir.exists()) throw new IllegalArgumentException("Directory " + path + " does not exist!");
    if (!model_names.exists()) throw new IllegalArgumentException("File model_names does not exist!");

    try {
      // Read in model names one by one
      BufferedReader br = new BufferedReader(new FileReader(model_names));
      String line;
      while((line = br.readLine()) != null) {
        // Load each model into H2O
        if(line.contains("main model : ")) {
          File modelFile = new File(parentDir, line.split(": ")[1]);
          model = new Model2FileBinarySerializer().load(modelFile);
        } else {
          File modelFile = new File(parentDir, line);
          new Model2FileBinarySerializer().load(modelFile);
        }
      }
    } catch( IOException e) {
      throw new IllegalArgumentException("Cannot load models ", e);
    }
  }

  private void loadFromHdfs() {
    if (FSUtils.isBareS3NBucketWithoutTrailingSlash(path)) { path += "/"; }
    Path parentDir = new Path(path);
    Path model_names = new Path(path, "model_names");
    try {
      FileSystem fs = FileSystem.get(parentDir.toUri(), PersistHdfs.CONF);

      if (!fs.exists(parentDir)) throw new IllegalArgumentException("HDFS Directory" + path + " does not exist!");
      if (!fs.exists(model_names)) throw new IllegalArgumentException("File model_names does not exist!");

      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(model_names)));
      String line;
      while((line = br.readLine()) != null) {
        if(line.contains("main model : ")) {
          Path modelFile = new Path(parentDir, line.split(": ")[1]);
          model = new Model2HDFSBinarySerializer(fs, false).load(modelFile);
        } else {
          Path modelFile = new Path(parentDir, line);
          new Model2HDFSBinarySerializer(fs, false).load(modelFile);
        }
      }


    } catch( IOException e ) {
      throw new IllegalArgumentException("Cannot load file " + path, e);
    }
  }
}
