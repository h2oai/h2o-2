package water.util;

import java.io.*;

import water.*;
import water.parser.ParseDataset;

import com.google.common.io.Closeables;

public class VAUtils {

  public static Key loadFile(File file) {
    return loadFile(file, file.getPath());
  }

  public static Key loadFile(File file, String keyname) {
    Key key = null;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      key = ValueArray.readPut(keyname, fis);
    } catch( IOException e ) {
      Closeables.closeQuietly(fis);
    }
    return key;
  }

  public static ValueArray parseKey(Key fileKey, Key parsedKey) {
    ParseDataset.parse(parsedKey, new Key[] { fileKey });
    return DKV.get(parsedKey).get();
  }
}
