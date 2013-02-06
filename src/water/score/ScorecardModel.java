package water.score;

import java.util.*;

/**
 * Scorecard model - decision table.
 *
 */
public class ScorecardModel {

  /** Name */
  final String _name;
  /** Output column */
  String _predictor;
  /** Initial score */
  final double _initialScore;

  /** Scorecard features */
  final Map<String, RuleTable> _features;

  private ScorecardModel(final String name, double initialScore) { _name = name; _initialScore = initialScore; _features = new HashMap<String, ScorecardModel.RuleTable>(); }

  /** Score this model on the specified row of data.  */
  public double score(final Map<String, Comparable> row ) {
    double score = _initialScore;
    for (String k : _features.keySet()) {
      RuleTable ruleTable = _features.get(k);
      if(ruleTable!=null) {
        System.out.println("ScorecardModel.score(): " + ruleTable.toString());
        score += ruleTable.score(row.get(k));
      }
    }
    return score;
  }

  /** Feature decision table */
  public static class RuleTable<T> {
    final String     _name;
    final Rule<T>[]  _rule;
    final DataTypes  _type;

    public RuleTable(final String name, final DataTypes type, final Rule<T>[] decisions) { _name = name; _type = type; _rule = decisions; }

    double score(T value) {
      /* The code introduced by cyprien, but I do not see test case for this flow.
       * But I leave here for now intentionally.
      if(value instanceof String) {
        switch(_type) {
          case BOOLEAN:
            value = (T) Boolean.valueOf((String) value);
            break;
          case INT:
            value = (T) new Long(Double.valueOf((String) value).longValue());
            break;
          case DOUBLE:
            value = (T) Double.valueOf((String) value);
            break;
          case STRING:
            break;
        }
      } else if(value instanceof Integer) {
        value = (T) new Long(((Integer) value).intValue());
      } else if(value instanceof Float) {
        value = (T) new Double(((Float) value).floatValue());
      } */

      double score = 0;
      for (Rule r : _rule) score += r.score(value);
      return score;
    }

    @Override
    public String toString() {
      return "RuleTable [_name=" + _name + ", _rule=" + Arrays.toString(_rule) + ", _type=" + _type + "]";
    }
  }

  /** Scorecard decision rule */
  public static class Rule<T> {
    final double _score;
    final Predicate<T> _predicate;
    public Rule(double score, Predicate<T> pred) { _score = score; _predicate = pred; }
    double score(T value) { return _predicate!=null && _predicate.match(value) ? _score : 0; }
    @Override public String toString() { return _predicate.toString() + " => " + _score; }
  }

  public static abstract class Predicate<T> {
    abstract boolean match(T value);
  }
  /** Less or equal */
  public static class LessOrEqual<T extends Comparable<T>> extends Predicate<T> {
    T _value;
    public LessOrEqual(T value) { _value = value; }
    @Override boolean match(T value) { return value!=null && _value.compareTo(value) >= 0; }
    @Override public String toString() { return "X<=" + _value; }
  }

  public static class LessThan<T extends Comparable<T>> extends LessOrEqual<T> {
    public LessThan(T value) { super(value); }
    @Override boolean match(T value) { return value!=null && _value.compareTo(value) > 0; }
    @Override public String toString() { return "X<" + _value; }
  }

  public static class GreaterOrEqual<T extends Comparable<T>> extends LessThan<T> {
    public GreaterOrEqual(T value) { super(value); }
    @Override boolean match(T value) { return value!=null && ! super.match(value); }
    @Override public String toString() { return "X>=" + _value; }
  }

  public static class GreaterThan<T extends Comparable<T>> extends LessOrEqual<T> {
    public GreaterThan(T value) { super(value); }
    @Override boolean match(T value) { return value!=null && ! super.match(value); }
    @Override public String toString() { return "X>" + _value; }
  }

  public static class IsMissing<T> extends Predicate<T> {
    @Override boolean match(T value) { return value==null; }
    @Override public String toString() { return "isMissing"; }
  }

  public static class Equals<T extends Comparable<T>> extends Predicate<T> {
    T _value;
    public Equals(T value) { _value = value; }
    @Override boolean match(T value) { return value!=null && _value.compareTo(value) == 0; }
    @Override public String toString() { return "X==" + _value; }
  }
  public static abstract class CompoundPredicate<T> extends Predicate<T> {
    Predicate<T> _l,_r;
    public final void add(Predicate<T> pred) {
      assert _l== null || _r==null : "Predicate already filled";
      if (_l==null) _l = pred; else _r = pred;
    }
  }
  public static class And<T> extends CompoundPredicate<T> {
    @Override final boolean match(T value) { return _l.match(value) && _r.match(value); }
    @Override public String toString() { return "(" + _l.toString() + " and " + _r.toString() + ")"; }
  }
  public static class Or<T> extends CompoundPredicate<T> {
    @Override final boolean match(T value) { return _l.match(value) || _r.match(value); }
    @Override public String toString() { return "(" + _l.toString() + " or " + _r.toString() + ")"; }
  }

  public static abstract class SetPredicate<T> extends Predicate<T> {
    public T[] _values;
    public SetPredicate(T[] value) { _values = value; }
  }

  public static class IsIn<T> extends SetPredicate<T> {
    public IsIn(T[] value) { super(value); }
    @Override boolean match(T value) {
      for (T t : _values) if (t.equals(value)) return true;
      return false;
    }
    @Override public String toString() {
      String x = "";
      for (T s: _values) x += s.toString() + " ";
      return "X is in {" + x + "}"; }
  }

  public static class IsNotIn<T> extends IsIn<T> {
    public IsNotIn(T[] value) { super(value); }
    @Override boolean match(T value) { return ! super.match(value); }
  }

  @Override
  public String toString() {
    return "ScorecardModel [_name=" + _name + ", _predictor=" + _predictor + ", _initialScore=" + _initialScore + "]";
  }

  /** Scorecard model builder */
  public static class Builder {
    ScorecardModel _scm;

    public Builder(final String name, double initialScore) { _scm = new ScorecardModel(name, initialScore); }
    public final ScorecardModel build() { return  _scm; }

    public final void addRuleTable(final String featureName, final DataTypes type, final List<Rule> rules) {
      _scm._features.put(featureName, new RuleTable(featureName, type, rules.toArray(new Rule[rules.size()])));
    }
  }

  /** Features datatypes promoted by PMML spec. */
  public enum DataTypes {
    DOUBLE, INT, BOOLEAN, STRING;
    public static DataTypes parse(String s) {return DataTypes.valueOf(s.toUpperCase()); }
  }
}
