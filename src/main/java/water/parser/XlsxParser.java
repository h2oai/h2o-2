package water.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import water.DKV;
import water.Key;
import water.util.Log;

import com.google.common.base.Strings;

public class XlsxParser extends CustomParser {

  public XlsxParser(ParserSetup setup) {super(setup);}
  public XlsxParser() {super(null);}

  private SharedStringsTable _sst;
  private boolean _firstRow;
  private ArrayList<String> _colNames = new ArrayList();
  private ValueString _str = new ValueString();
  private CustomParser.DataOut _dout;

  public XlsxParser clone() {return new XlsxParser(_setup);}
  private XMLReader makeSheetParser() throws SAXException {
    XMLReader parser = XMLReaderFactory.createXMLReader();
    parser.setContentHandler(new SheetHandler());
    return parser;
  }

  @Override public DataOut streamParse(InputStream is, CustomParser.DataOut dout){
    _firstRow = true;
    _dout = dout;
    try {
      XSSFReader reader = new XSSFReader(OPCPackage.open(is));
      _sst = reader.getSharedStringsTable();
      XMLReader parser = makeSheetParser();
      Iterator<InputStream> it = reader.getSheetsData();
      while (it.hasNext()) {
        InputStream sheet = it.next();
        try {
          parser.parse(new InputSource(sheet));
        } finally {
          sheet.close();
        }
      }
    }catch(Exception e){
      throw new RuntimeException(e);
    } finally {
      try { is.close(); } catch (IOException e) { Log.err(e); }
    }
    return dout;
  }

  private class SheetHandler extends DefaultHandler {
    private String  _lastContents;
    private boolean _nextIsString;
    private int _curCol;
    private String _rowStr;

    public void startElement(String uri, String localName, String name,
        Attributes attributes) throws SAXException {
      // c => cell
      if( name.equals("row") ) {
        _rowStr = Strings.nullToEmpty(attributes.getValue("r"));
      } else if( name.equals("c") ) {
        // Figure out if the value is an index in the SST
        String cellType = attributes.getValue("t");
        if( cellType != null && cellType.equals("s") ) {
          _nextIsString = true;
        } else {
          _nextIsString = false;
        }

        String cell = attributes.getValue("r");
        cell = cell.substring(0, cell.length() - _rowStr.length());
        _curCol = 0;
        for( int i = 0; i < cell.length(); ++i ) {
          _curCol *= 26;
          char c = cell.charAt(i);
          assert 'A' <= c && c <= 'Z';
          _curCol += c - 'A';
        }
      }
      // Clear contents cache
      _lastContents = "";
    }

    public void endElement(String uri, String localName, String name)
        throws SAXException {
      // Process the last contents as required.
      // Do now, as characters() may be called more than once
      if( _nextIsString ) {
        int idx = Integer.parseInt(_lastContents);
        _lastContents = new XSSFRichTextString(_sst.getEntryAt(idx)).toString();
        _nextIsString = false;
      }

      // v => contents of a cell
      // Output after we've seen the string contents
      if( name.equals("v") ) {
        if (_firstRow) {
          _colNames.add(_lastContents);
        } else {
          try {
            try {
              Double d = Double.parseDouble(_lastContents);
              if (Double.isNaN(d))
                _dout.addInvalidCol(_curCol);
              else
                _dout.addNumCol(_curCol,d);
            } catch( NumberFormatException e ) {
              if (_lastContents.isEmpty())
                _dout.addInvalidCol(_curCol);
              else
                _dout.addStrCol(_curCol, _str.setTo(_lastContents));
            }
          } catch (Exception e) {
            Log.err(e);
          }
        }
      } else if( name.equals("row") ) {
        if( _firstRow == true ) {
          _dout.setColumnNames(_colNames.toArray(new String[_colNames.size()]));
          _firstRow = false;
        } else {
          _dout.newLine();
        }
      }
    }

    public void characters(char[] ch, int start, int length)
        throws SAXException {
      _lastContents += new String(ch, start, length);
    }
  }

  @Override public boolean isCompatible(CustomParser p) {
    return p instanceof XlsxParser;
  }
}