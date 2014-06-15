package water.serial;

import java.io.*;
import java.nio.channels.FileChannel;

import water.*;
import water.util.Utils;

/**
 * Model serializer targeting file based output.
 */
public class Model2FileBinarySerializer extends BinarySerializer<Model, File, File> {


  @Override public void save(Model m, File f) throws IOException {
    assert m!=null : "Model cannot be null!";
    FileOutputStream fo = null;
    AutoBuffer ab = null;
    try {
      fo = new FileOutputStream(f);
      m.getModelSerializer().save(m,
          saveHeader( m, ab=ab4write(fo.getChannel()) ) );
    } catch( FileNotFoundException e ) {
      throw new IllegalArgumentException("Cannot open given file!", e);
    } finally {
      if (ab!=null) ab.close();
      Utils.close(fo);
    }
  }

  @Override public Model load(File f) throws IOException {
    FileInputStream fi = null;
    AutoBuffer ab = null;
    Model m = null;
    try {
      fi = new FileInputStream(f);
      m = loadHeader(ab=ab4read(fi.getChannel()));
      m.getModelSerializer().load(m, ab);
      if (m._key!=null) {
        DKV.put(m._key, m);
      }
    } catch( FileNotFoundException e ) {
      throw new IllegalArgumentException("Cannot open given file!", e);
    } finally {
      if (ab!=null) ab.close();
      Utils.close(fi);
    }
    return m;
  }

  @Override public Model load(Model m, File f) throws IOException {
    throw new UnsupportedOperationException();
  }

  private AutoBuffer ab4read  (FileChannel fc) { return new AutoBufferWithoutTypeIds(fc, true); }
  private AutoBuffer ab4write (FileChannel fc) { return new AutoBufferWithoutTypeIds(fc, false); }
}
