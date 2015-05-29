package water.api;

import java.io.*;

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
import water.util.JCodeGen;

public class SaveModel extends Func {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help = "Model to save.", required=true, filter=Default.class)
  Model model;

  @API(help = "Path of directory to save model(s)", required = true, filter = Default.class, json=true, gridable = false)
  String path;

  @API(help="Overwrite existing files.", required = false, filter = Default.class, gridable = false)
  boolean force = false;

  @API(help="Save cross-validation models.", required = false, filter = Default.class, gridable = false)
  boolean save_cv = true;

  @Override
  protected void execImpl() {
    if (isHdfs(path) || isS3N(path)) saveToHdfs();
    else saveToLocalFS();
  }

  private void saveToLocalFS() {
    File parentDir = new File(path);
    if (!force && parentDir.exists()) throw new IllegalArgumentException("The file " + path + " already exists!");
    try {
      // If force is specified then delete the file f
      if (force && parentDir.exists()) delete(parentDir);
      // Create folder
      parentDir.mkdirs();
      // Save parent model
      new Model2FileBinarySerializer().save(model, new File(parentDir, JCodeGen.toJavaId(model._key.toString())));
      // Write to model_names
      File model_names = new File(parentDir, "model_names");
      FileOutputStream is = new FileOutputStream(model_names);
      OutputStreamWriter osw = new OutputStreamWriter(is);
      BufferedWriter br = new BufferedWriter(osw);
      br.write("main model : " + model._key.toString());
      br.newLine();
      // Save cross validation models
      if (save_cv) {
        Model[] models = getCrossValModels(model);
        System.out.println(models);
        for (Model m : models) {
          new Model2FileBinarySerializer().save(m, new File(parentDir, JCodeGen.toJavaId(m._key.toString())));
          br.write(JCodeGen.toJavaId(m._key.toString()));
          br.newLine();
        }
      }
      br.close();
    } catch( IOException e ) {
      throw new IllegalArgumentException("Cannot save file " + path, e);
    }
  }

  private void saveToHdfs() {
    if (FSUtils.isBareS3NBucketWithoutTrailingSlash(path)) { path += "/"; }
    Path parentDir = new Path(path);
    try {
      FileSystem fs = FileSystem.get(parentDir.toUri(), PersistHdfs.CONF);
      if (force && fs.exists(parentDir)) fs.delete(parentDir);
      fs.mkdirs(parentDir);
      // Save parent model
      new Model2HDFSBinarySerializer(fs, force).save(model, new Path(parentDir, JCodeGen.toJavaId(model._key.toString())));
      // Save parent model key to model_names file
      Path model_names = new Path(parentDir, "model_names");
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(model_names,true)));
      br.write("main model : " + model._key.toString());
      br.newLine();
      if (save_cv) {
        Model[] models = getCrossValModels(model);
        for (Model m : models ) {
          new Model2HDFSBinarySerializer(fs, force).save(m, new Path(parentDir, JCodeGen.toJavaId(m._key.toString())));
          br.write(JCodeGen.toJavaId(m._key.toString()));
          br.newLine();
        }
      }
      br.close();
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

  private void delete(File f) {
    if (!f.isDirectory()) {
      f.delete();
    } else {
      File[] contents = f.listFiles();
      for(File ef : contents){
        delete(ef);
      }
      f.delete();
    }
  }

  private Model[] getCrossValModels(Model m) {
    Model[] models = null;

    if (m instanceof GLMModel && ((GLMModel) m).xvalModels() == null ) {
      models = NO_MODELS;
    } else if (m instanceof GLMModel && ((GLMModel) m).xvalModels().length > 0) {
      Key[] keys = ((GLMModel) m).xvalModels();
      models = new Model[keys.length];
      int i = 0;
      for (Key k : keys) {
        models[i++] = UKV.get(k);
      }
    } else {
      if (m.hasCrossValModels()) {
        Job.ValidatedJob j = (Job.ValidatedJob) m.get_params();
        models = new Model[j.xval_models.length];
        int i = 0;
        for (Key k : j.xval_models) {
          System.out.println(k);
          models[i++] = UKV.get(k);
        }
      } else {
        models = NO_MODELS;
      }
    }
    assert models != null;
    return models;
  }

  private static final Model[] NO_MODELS = new Model[] {};
}
