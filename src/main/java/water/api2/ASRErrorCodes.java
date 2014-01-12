package water.api2;

/**
 * Error codes and messages.
 */
public class ASRErrorCodes {
  // Error codes.
  public static int INTERNAL_SERVER_ERROR = 500;
  public static int MISSING_REQUIRED_PARAMETER = 1000;
  public static int UNKNOWN_PARAMETER = 1001;

  public static String getTitle(int code) {
    if (code == INTERNAL_SERVER_ERROR) {
      return "Internal server error";
    }
    else if (code == MISSING_REQUIRED_PARAMETER) {
      return "Required parameter missing";
    }
    else if (code == UNKNOWN_PARAMETER) {
      return "Unknown parameter";
    }
    return null;
  }
}
