package water;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import water.util.Log;

public class ValueArrayToFrameTestAll extends TestUtil {
  @BeforeClass public static void stall() {
    stall_till_cloudsize(3);
  }

  // @Test (HEX-854)
  public void allstate() {
    File f = find_test_file("datasets/allstate/train_set.zip");
    ValueArrayToFrameTest.test(f.getAbsolutePath());
  }

  @Test public void smalldata() {
    File f = find_test_file("smalldata");
    ArrayList<String> paths = new ArrayList<String>();
    findDatasets(f, paths);
    Collections.sort(paths); // Predictable tests
    for( String path : paths ) {
      Log.info("ValueArrayToFrame: " + path);
      ValueArrayToFrameTest.test(path);
    }
  }

  static void findDatasets(File folder, ArrayList<String> paths) {
    for( File file : folder.listFiles() ) {
      if( file.isDirectory() )
        findDatasets(file, paths);
      else {
        if( file.getPath().endsWith(".csv") || //
            file.getPath().endsWith(".data") || //
            file.getPath().endsWith(".dat") || //
            file.getPath().endsWith(".xls") || //
            file.getPath().endsWith(".zip") || //
            file.getPath().endsWith(".gz") ) {
          if( true && //
              // Creates one column of all NA -> cannot create CSV
              !file.getPath().endsWith("mixed_causes_NA.csv") &&
              // Uses C0LChunk, which doesn't keep track of NAs
              !file.getPath().endsWith("badchars.csv") &&
              //
              !file.getPath().endsWith("datagen1.csv") && //
              !file.getPath().endsWith("sdss174052.csv.gz") && //
              !file.getPath().endsWith("hex-443.parsetmp_1_0_0_0.data") && //
              !file.getPath().endsWith("iris_header.csv") && //
              !file.getPath().endsWith("parse_fail_double_space.csv") && //
              !file.getPath().endsWith("random1csv.data") && //
              true )
            paths.add(file.getAbsolutePath());
        }
      }
    }
  }
}
