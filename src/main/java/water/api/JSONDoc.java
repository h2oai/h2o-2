package water.api;

/**
 * Meta-info on API classes documenting their JSON behavior
 */
public class JSONDoc {
  final String _name;           // JSON field name
  final String _help;           // Some descriptive text
  final int _min_ver, _max_ver; // Min/Max supported-version numbers
  final Class _clazz;           // Java type
  public JSONDoc( String name, String help, int min, int max, Class C ) {
    _name = name; _help = help; _min_ver = min; _max_ver = max; _clazz = C;
  }
  @Override public String toString() {
    return "{"+_name+", from "+_min_ver+" to "+_max_ver+", "+_clazz+", "+_help+"}";
  }
}
