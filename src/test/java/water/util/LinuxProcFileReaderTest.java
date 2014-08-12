package water.util;

import org.junit.Assert;
import org.junit.Test;

public class LinuxProcFileReaderTest {
  @Test public void numSetBitsHexTest() {
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("0"), 0);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("1"), 1);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("8"), 1);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("ffffffff"), 32);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("fffffffff"), 36);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("efffffffff"), 39);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("7fffffffff"), 39);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("ffffffffff"), 40);
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("7000000000000000000000001"), (3+1));
    Assert.assertEquals(LinuxProcFileReader.numSetBitsHex("0007000000d00000000000000000000"), (3+3));
  }
}
