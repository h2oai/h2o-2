package water.api2;

import water.NanoHTTPD.Response.Status;

/**
 * Response information when an parameter error is returned.
 * This class is used for json marshaling.
 *
 * http://tools.ietf.org/html/draft-nottingham-http-problem-02
 */
public class ASRErrorInfo {
  // Required members.
  public String describedBy;
  public String title;

  // Optional members.
  public int httpStatus;
  public String detail;

  // Extension members.
  /**
   * See ASRErrorCodes.
   */
  public int errorCode;
  public String exceptionClassName;
  public String exceptionErrorMessage;
  public String[] exceptionStackTrace;

  // Private members (not marshaled).
  private transient Status _httpStatus;

  /**
   * Constructor.
   * @param errorCode Error code.  (From AbstractSimpleRequestHandler.ASR_ERROR_*)
   * @param detail Error message detail.
   */
  public ASRErrorInfo(int errorCode, Status httpStatus, String detail) {
    this.errorCode = errorCode;
    this._httpStatus = httpStatus;
    this.detail = detail;

    this.describedBy = "";
    this.title = ASRErrorCodes.getTitle(errorCode);
    this.httpStatus = this._httpStatus.getRequestStatus();
  }

  /**
   * Set the Exception to report for this error case.
   * @param e The exception.
   */
  public void setException(Exception e) {
    exceptionClassName = e.getClass().getName();
    exceptionErrorMessage = (e.getMessage() != null) ? e.getMessage() : "";
    StackTraceElement elements[] = e.getStackTrace();
    exceptionStackTrace = new String[elements.length];
    for (int i = 0; i < elements.length; i++) {
      StackTraceElement el = elements[i];
      String s = el.toString();
      exceptionStackTrace[i] = s;
    }
  }

  /**
   * Get the previously set http status.
   * @return The http status.
   */
  public Status getHttpStatus() { return _httpStatus; }
}
