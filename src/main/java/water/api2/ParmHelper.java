package water.api2;

import water.NanoHTTPD.Response.Status;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for validating and extracting parameters.
 */
public class ParmHelper {
  private Map<String, String> _map;
  private Set<String> _visited;

  /**
   * Constructor.
   * @param parmsMap Parameters provided by the session.
   */
  public ParmHelper(Map<String, String> parmsMap) {
    _map = parmsMap;
    _visited = new HashSet<String>();
  }

  /**
   * Get a required parameter as a string.
   *
   * @param name Parameter name
   * @return Parameter value.  Guaranteed not to be null.
   * @throws ASRIllegalArgumentException
   */
  public String getRequiredStringParm(String name) throws ASRIllegalArgumentException {
    String value = _map.get(name);
    if (value == null) {
      ASRIllegalArgumentException e =
        new ASRIllegalArgumentException(
                new ASRErrorInfo(
                        ASRErrorCodes.MISSING_REQUIRED_PARAMETER,
                        Status.BAD_REQUEST,
                        "Missing required parameter with name " + name + "."));
      throw e;
    }
    _visited.add(name);
    return value;
  }

  /**
   * Get an optional parameter as a string.
   *
   * @param name Parameter name
   * @return Parameter value if it exists; null otherwise.
   * @throws ASRIllegalArgumentException
   */
  public String getOptionalStringParm(String name) throws ASRIllegalArgumentException {
    _visited.add(name);

    if (_map.containsKey(name)) {
      String value = _map.get(name);
      return value;
    }

    return null;
  }

  /**
   * Do checks after all parameters have been read.
   *
   * @throws ASRIllegalArgumentException
   */
  public void check() throws ASRIllegalArgumentException {
    // Check for extra parameters.
    for (String key : _map.keySet()) {
      if (! _visited.contains(key)) {
        ASRIllegalArgumentException e =
                new ASRIllegalArgumentException(
                        new ASRErrorInfo(
                                ASRErrorCodes.UNKNOWN_PARAMETER,
                                Status.BAD_REQUEST,
                                "Unknown parameter with name " + key + "."));
        throw e;
      }
    }
  }
}
