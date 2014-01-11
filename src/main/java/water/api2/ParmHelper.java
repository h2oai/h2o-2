package water.api2;

import water.NanoHTTPD;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ParmHelper {
  private Map<String, String> _map;
  private Set<String> _visited;

  public ParmHelper(Map<String, String> parmsMap) {
    _map = parmsMap;
    _visited = new HashSet<String>();
  }

  public String getRequiredStringParm(String name) throws ASRIllegalArgumentException {
    String value = _map.get(name);
    if (value == null) {
      ASRIllegalArgumentException e =
        new ASRIllegalArgumentException(
                new ASRArgumentErrorInfo(
                        AbstractSimpleRequestHandler.ASR_ERROR_MISSING_REQUIRED_PARAMETER,
                        "Missing required parameter",
                        name));
      throw e;
    }
    _visited.add(name);
    return value;
  }

  public String getOptionalStringParm(String name) throws IllegalArgumentException {
    _visited.add(name);

    if (_map.containsKey(name)) {
      String value = _map.get(name);
      return value;
    }

    return null;
  }

  public void check() throws IllegalArgumentException {
    // Check for extra parameters.
    for (String key : _map.keySet()) {
      if (! _visited.contains(key)) {
        ASRIllegalArgumentException e =
                new ASRIllegalArgumentException(
                        new ASRArgumentErrorInfo(
                                AbstractSimpleRequestHandler.ASR_ERROR_UNKNOWN_PARAMETER,
                                "Parameter is unknown",
                                key));
        throw e;
      }
    }
  }
}
