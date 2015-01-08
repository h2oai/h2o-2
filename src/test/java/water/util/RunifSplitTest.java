package water.util;

import org.junit.Assert;
import org.junit.Test;
import water.Key;
import water.TestUtil;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

public class RunifSplitTest extends TestUtil {
  final static String PATH = "smalldata/iris/iris.csv";

  @Test
  public void test1() {
    Key file = NFSFileVec.make(find_test_file(PATH));
    Frame fr = ParseDataset2.parse(Key.make("iris_nn2"), new Key[]{file});
    Frame[] split = Frame.runifSplit(fr, .70f, -1);
    Assert.assertTrue(split[0].numRows() + split[1].numRows() == fr.numRows());
    fr.delete();
    split[0].delete();
    split[1].delete();
  }
}
