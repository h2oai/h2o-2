package water.api2;

/**
 * Response information when an parameter error is returned.
 * This class is used for json marshaling.
 */
public class ASRArgumentErrorInfo {
  public int error_code;
  public String error_message;
  public String error_parameter;

  /**
   * Constructor.
   * @param c Error code.  (From AbstractSimpleRequestHandler.ASR_ERROR_*)
   * @param m Error message.
   * @param p Error parameter.
   */
  public ASRArgumentErrorInfo(int c, String m, String p) {
    error_code = c;
    error_message = m;
    error_parameter = p;
  }
}