package water.api;

import static water.util.FSUtils.isHdfs;
import static water.util.FSUtils.isS3N;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import water.Func;
import water.Model;
import water.persist.PersistHdfs;
import water.serial.Model2FileBinarySerializer;
import water.serial.Model2HDFSBinarySerializer;
import water.util.FSUtils;

public class SaveModel extends Func {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Model to save.", required=true, filter=Default.class)
  Model model;

  @API(help = "Name of file to save the model.", required = true, filter = Default.class, json=true, gridable = false)
  String path;

  @API(help="Overwrite existing files.", required = false, filter = Default.class, gridable = false)
  boolean force = false;

  @Override
  protected void execImpl() {
    if (isHdfs(path) || isS3N(path)) saveToHdfs();
    else saveToLocalFS();
  }

  private void saveToLocalFS() {
    File f = new File(path);
    if (!force && f.exists()) throw new IllegalArgumentException("The file " + path + " already exists!");

    try {
      new Model2FileBinarySerializer().save(model, new File(path));
    } catch( IOException e ) {
      throw new IllegalArgumentException("Cannot save file " + path, e);
    }
  }

  private void saveToHdfs() {
    if (FSUtils.isBareS3NBucketWithoutTrailingSlash(path)) { path += "/"; }
    Path f = new Path(path);
    try {
      FileSystem fs = FileSystem.get(f.toUri(), PersistHdfs.CONF);
      new Model2HDFSBinarySerializer(fs, force).save(model, f);
    } catch( IOException e ) {
      throw new IllegalArgumentException("Cannot save file " + path, e);
    }
  }

  @Override public boolean toHTML(StringBuilder sb) {
    sb.append("<div class=\"alert alert-success\">")
      .append("Model ")
      .append(Inspector.link(model._key.toString(), model._key.toString()))
      .append(" was sucessfuly saved to <b>"+path+"</b> file.");
    sb.append("</div>");
    return true;
  }
}
