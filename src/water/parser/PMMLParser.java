package water.parser;

import water.score.*;
import water.score.ScorecardModel.CompoundPredicate;
import water.score.ScorecardModel.DataTypes;
import water.score.ScorecardModel.IsNotIn;
import water.score.ScorecardModel.Predicate;
import water.score.ScorecardModel.Rule;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.xml.parsers.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import water.H2O;
import water.Key;

public class PMMLParser extends CustomParser {

  @Override
  public void parse(final Key key) throws Exception {
    H2O.unimpl();
  }

  public static ScorecardModel load(final InputStream is) throws Exception {
    PMMLSaxParser pmmlParser = new PMMLSaxParser(is);

    return pmmlParser.parseDocument();
  }

  static class PMMLSaxParser extends DefaultHandler {
    final InputStream _is;
    /** Constructed Scorecard model */
    ScorecardModel.Builder _scmb;
    Map<String, DataTypes> _featureTypes;

    /* Actual parsing state */
    private String     _name;   // table name
    private List<Rule> _rules;  // list of rules
    private double     _score;  // score for current decision rules
    private Predicate  _predicate;
    private Stack<Predicate> _predicates;
    private boolean    _needContent;

    public PMMLSaxParser(final InputStream is) {
      _is = is; _scmb = null;
      _rules        = new ArrayList<ScorecardModel.Rule>();
      _predicates   = new Stack<ScorecardModel.Predicate>();
      _featureTypes = new HashMap<String, DataTypes>();
      _needContent  = false;
    }

    ScorecardModel parseDocument() throws SAXException, IOException, ParserConfigurationException {
      SAXParserFactory saxFactory = SAXParserFactory.newInstance();
      SAXParser saxParser = saxFactory.newSAXParser();
      saxParser.parse(_is, this);

      return _scmb.build();
    }

    @Override public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
      if (qName.equals("DataField"))
        _featureTypes.put(attributes.getValue("name"), DataTypes.parse(attributes.getValue("dataType")));
      else if (qName.equals("Scorecard"))
        _scmb = new ScorecardModel.Builder(attributes.getValue("modelName"), Double.valueOf(attributes.getValue("initialScore")));
      else if (qName.equals("Characteristic")) {
        assert _name == null;
        assert _rules.isEmpty();
      } else if (qName.equals("Attribute")) {
        assert _predicates.isEmpty();
        _score = Double.valueOf(attributes.getValue("partialScore"));
      } else if (qName.equals("SimplePredicate")) {
        addSimplePred(attributes.getValue("field"), Operators.valueOf(attributes.getValue("operator")), attributes.getValue("value"));
      } else if (qName.equals("CompoundPredicate")) {
        addCompoundPred(BooleanOperators.valueOf(attributes.getValue("booleanOperator")));
      } else if (qName.equals("SimpleSetPredicate")) {
        addSimpleSetPred(attributes.getValue("field"), BooleanOperators.valueOf(attributes.getValue("booleanOperator")));
      }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      if (qName.equals("Characteristic")) {
        assert _featureTypes.containsKey(_name);
        _scmb.addRuleTable(_name, _featureTypes.get(_name), _rules);
        _name = null;
        _rules.clear();
      } else if (qName.equals("Attribute")) {
        _rules.add(new Rule(_score, _predicate));
        _predicate = null;
      } else if (qName.equals("CompoundPredicate") ) {
        Predicate rite = _predicates.pop();
        Predicate left = _predicates.pop();
        CompoundPredicate cp = (CompoundPredicate)_predicates.peek();
        cp.add(left);
        cp.add(rite);
        
      } else if (qName.equals("SimpleSetPredicate") || qName.equals("SimplePredicate")) {
        _needContent = false;
      }
    }

    private void addSimplePred(String featureName, Operators op, String value) {
      assert _name == null || _name.equals(featureName) : "PMML is not in expected format!";
      _name = featureName;

      DataTypes ft = _featureTypes.get(featureName);
      Predicate pred = null;
      if (value != null) {
        switch (ft) {
        case DOUBLE  : pred = getSimplePred(op, Double.valueOf(value)); break;
        case BOOLEAN : pred = getSimplePred(op, Boolean.valueOf(value)); break;
        case INT     : pred = getSimplePred(op, Double.valueOf(value).longValue()); break;
        case STRING  : pred = getSimplePred(op, value); break;
        }
      } else {
        assert op == Operators.isMissing;
        pred = new ScorecardModel.IsMissing();
      }
      addPred(pred);
    }

    private <T extends Comparable<T>> Predicate<T> getSimplePred(Operators op, T value) {
     switch (op) {
      case lessOrEqual   : return new ScorecardModel.LessOrEqual(value);
      case lessThan      : return new ScorecardModel.LessThan(value);
      case greaterOrEqual: return new ScorecardModel.GreaterOrEqual(value);
      case greaterThan   : return new ScorecardModel.GreaterThan(value);
      case equal         : return new ScorecardModel.Equals(value);
      default            : return null;
     }
    }

    private void addCompoundPred(BooleanOperators op) {
      Predicate pred = null;
      switch( op ) {
      case and: pred = new ScorecardModel.And(); break;
      case or : pred = new ScorecardModel.Or();  break;
      default : pred = null; break;
      }
      addPred(pred);
    }

    private void addSimpleSetPred(String featureName, BooleanOperators op) {
      assert _name == null || _name.equals(featureName) : "PMML is not in expected format!";
      _name = featureName;

      Predicate pred = null;
      switch(op) {
      case isNotIn: pred = new ScorecardModel.IsNotIn((String[]) null); _needContent = true; break;
      default     : pred = null;
      }
      addPred(pred);
    }

    @Override public void characters(char[] ch, int start, int length) throws SAXException {
      if (_needContent) {
        // Only SetSimplePredicate needs to handle tag content
        String      val = new String(ch, start, length);
        String[] values = val.split(" ");

        ScorecardModel.IsNotIn<String> pred = (IsNotIn<String>) _predicate;
        pred._values = values;
      }
    }

    private void addPred(final Predicate pred) {
      assert pred != null;
      _predicate = pred;
      _predicates.push(pred);
    }

    enum Operators {
      lessOrEqual, lessThan, greaterOrEqual, greaterThan, equal, isMissing
    }
    enum BooleanOperators {
      isNotIn, and, or
    }
  }
}
