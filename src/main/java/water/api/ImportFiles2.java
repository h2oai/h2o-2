package water.api;

import java.io.File;

import water.util.FileIntegrityChecker;

public class ImportFiles2 extends ImportFiles {
  @Override FileIntegrityChecker load(File path) {
    return FileIntegrityChecker.check(path, true);
  }
}
