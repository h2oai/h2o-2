package water.license;

import water.util.Log;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LicenseManager {
  public static final String FEATURE_GLM_SCORING = "glm_scoring";
  public static final String FEATURE_GBM_SCORING = "gbm_scoring";
  public static final String FEATURE_RF_SCORING  = "rf_scoring";
  public static final String FEATURE_DEEPLEARNING_SCORING = "deeplearning_scoring";

  public enum Result {
    OK,
    FILE_ERROR,
    SIGNATURE_ERROR,
    EXPIRED_ERROR
  }

  private String _license;

  public LicenseManager() {}

  private String readFile(String fileName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    try {
      StringBuilder sb = new StringBuilder();
      String line = br.readLine();

      while (line != null) {
        sb.append(line);
        sb.append("\n");
        line = br.readLine();
      }
      return sb.toString();
    } finally {
      br.close();
    }
  }

  public Result readLicenseFile(String fileName) {
    // Check if there is a file issue.
    String s;
    try {
      s = readFile(fileName);
    }
    catch (Exception e) {
      Log.err("readFile failed", e);
      return Result.FILE_ERROR;
    }

    // Check if license is not correctly signed.
    // Check if license is expired.

    _license = s;

    return Result.OK;
  }

  public void logLicensedFeatures() {
    ArrayList<String> featureNameList = new ArrayList<String>();
    featureNameList.add(FEATURE_DEEPLEARNING_SCORING);
    featureNameList.add(FEATURE_GBM_SCORING);
    featureNameList.add(FEATURE_GLM_SCORING);
    featureNameList.add(FEATURE_RF_SCORING);

    boolean silent = true;
    for (String featureName : featureNameList) {
      boolean b = isFeatureAllowed(featureName, silent);
      Log.info("isFeatureAllowed(" + featureName + "): " + b);
    }
  }

  public boolean isFeatureAllowed(String featureName) {
    boolean silent = false;
    return isFeatureAllowed(featureName, silent);
  }

  public boolean isFeatureAllowed(String featureName, boolean silent) {
    return true;
  }

  /*
  public boolean isFeatureAllowed(String featureName, boolean silent) {
    if (_license == null) {
      if (! silent) {
        Log.info("isFeatureAllowed(" + featureName + ") is false (no valid license found)");
      }

      return false;
    }

    Pattern p = Pattern.compile("Feature:\\s*(\\S*)\\s*");
    Scanner scanner = new Scanner(_license);
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      Matcher m = p.matcher(line);
      boolean b = m.matches();
      if (! b) {
        continue;
      }

      String lineFeatureName = m.group(1);
      if (featureName.equals(lineFeatureName)) {
        return true;
      }
    }
    scanner.close();

    if (! silent) {
      Log.info("isFeatureAllowed(" + featureName + ") is false (feature not licensed)");
    }

    return false;
  }
  */
}
