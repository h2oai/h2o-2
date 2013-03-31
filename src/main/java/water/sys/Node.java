package water.sys;

import java.io.IOException;

public interface Node {
  void inheritIO();

  void persistIO(String outFile, String errFile) throws IOException;

  void start();

  int waitFor();

  void kill();
}
