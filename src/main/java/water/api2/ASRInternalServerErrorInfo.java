package water.api2;

/**
 * Response information when an 500 internal server error is returned.
 * This class is used for json marshaling.
 */
public class ASRInternalServerErrorInfo {
  public String error_message;
  public String stack_trace;

  /**
   * Constructor.
   * @param m Error message.
   * @param t Stack trace.
   */
  public ASRInternalServerErrorInfo(String m, String t) {
    error_message = m;
    stack_trace = t;
  }
}
