package water.parser;

import java.io.*;
import java.util.ArrayList;

import org.apache.poi.hssf.eventusermodel.*;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.record.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import water.util.Log;
import water.util.Log.Tag.Sys;

public class XlsParser extends CustomParser implements HSSFListener {

  private transient POIFSFileSystem _fs;
  private transient FormatTrackingHSSFListener _formatListener;
  private transient final ValueString _str = new ValueString();
  private transient CustomParser.DataOut _dout;

  public XlsParser(){super(new ParserSetup(ParserType.XLS,CsvParser.AUTO_SEP,0,false,null,false));}
  public XlsParser(CustomParser.ParserSetup setup){super(null);}
  public XlsParser clone(){return new XlsParser(_setup);}

  @Override
  public DataOut streamParse( final InputStream is, final DataOut dout) throws Exception {
    _dout = dout;
    _firstRow = true;
    try {
      _fs = new POIFSFileSystem(is);
      MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(this);
      _formatListener = new FormatTrackingHSSFListener(listener);
      HSSFEventFactory factory = new HSSFEventFactory();
      HSSFRequest request = new HSSFRequest();
      request.addListenerForAllRecords(_formatListener);
      factory.processWorkbookEvents(request, _fs);
    } finally {
      try { is.close(); } catch (IOException e) { }
    }
    return dout;
  }

  /**
   * Try to parse the bits as svm light format, return SVMParser instance if the input is in svm light format, null otherwise.
   * @param bits
   * @return SVMLightPArser instance or null
   */
  public static PSetupGuess guessSetup(byte [] bits){
    InputStream is = new ByteArrayInputStream(bits);
    XlsParser p = new XlsParser();
    CustomInspectDataOut dout = new CustomInspectDataOut();
    try{p.streamParse(is, dout);}catch(Exception e){}
    return new PSetupGuess(new ParserSetup(ParserType.XLS,CsvParser.AUTO_SEP,dout._ncols, dout._header,dout._header?dout.data()[0]:null,false),dout._nlines,dout._invalidLines,dout.data(),dout._nlines > dout._invalidLines,null);
  }

  transient ArrayList<String> _columnNames = new ArrayList();
  boolean _firstRow;

  @Override
  public void processRecord(Record record) {
    int curCol = -1;
    double curNum = Double.NaN;
    ValueString curStr = null;

    switch( record.getSid() ) {
      case BoundSheetRecord.sid:
      case BOFRecord.sid:
        // we just run together multiple sheets
        break;
      case SSTRecord.sid:
        _sstRecord = (SSTRecord) record;
        break;
      case BlankRecord.sid:
        BlankRecord brec = (BlankRecord) record;

        curCol = brec.getColumn();
        curStr = _str.setTo("");
        break;
      case BoolErrRecord.sid:
        BoolErrRecord berec = (BoolErrRecord) record;

        curCol = berec.getColumn();
        curStr = _str.setTo("");
        break;

      case FormulaRecord.sid:
        FormulaRecord frec = (FormulaRecord) record;

        curCol = frec.getColumn();
        curNum = frec.getValue();

        if( Double.isNaN(curNum) ) {
          // Formula result is a string
          // This is stored in the next record
          _outputNextStringRecord = true;
          _nextCol = frec.getColumn();
        }
        break;
      case StringRecord.sid:
        if( _outputNextStringRecord ) {
          // String for formula
          StringRecord srec = (StringRecord) record;
          curStr = _str.setTo(srec.getString());
          curCol = _nextCol;
          _outputNextStringRecord = false;
        }
        break;
      case LabelRecord.sid:
        LabelRecord lrec = (LabelRecord) record;

        curCol = lrec.getColumn();
        curStr = _str.setTo(lrec.getValue());
        break;
      case LabelSSTRecord.sid:
        LabelSSTRecord lsrec = (LabelSSTRecord) record;
        if( _sstRecord == null ) {
          Log.warn(Sys.EXCEL,"[ExcelParser] Missing SST record");
        } else {
          curCol = lsrec.getColumn();
          curStr = _str.setTo(_sstRecord.getString(lsrec.getSSTIndex()).toString());
        }
        break;
      case NoteRecord.sid:
        Log.warn(Sys.EXCEL,"Warning cell notes are unsupported");
        break;
      case NumberRecord.sid:
        NumberRecord numrec = (NumberRecord) record;
        curCol = numrec.getColumn();
        curNum = numrec.getValue();
        break;
      case RKRecord.sid:
        Log.warn(Sys.EXCEL,"Warning RK records are unsupported");
        break;
      default:
        break;
    }

    // Handle missing column
    if( record instanceof MissingCellDummyRecord ) {
      MissingCellDummyRecord mc = (MissingCellDummyRecord) record;
      curCol = mc.getColumn();
      curNum = Double.NaN;
    }

    // Handle end of row
    if( record instanceof LastCellOfRowDummyRecord ) {
      if (_firstRow) {
        _firstRow = false;
        String[] arr = new String[_columnNames.size()];
        arr = _columnNames.toArray(arr);
        _dout.setColumnNames(arr);
      } else {
        _dout.newLine();
        curCol = -1;
      }
    }

    if (curCol == -1)
      return;

    if (_firstRow) {
      _columnNames.add(curStr == null ? ("C" + (curCol+1)) : curStr.toString());
    } else {
      if (curStr == null)
        if (Double.isNaN(curNum))
          _dout.addInvalidCol(curCol);
        else
          _dout.addNumCol(curCol, curNum);
      else
        _dout.addStrCol(curCol, curStr);
    }
  }

  private transient  SSTRecord _sstRecord;
  private int _nextCol;
  private boolean _outputNextStringRecord;

  @Override public boolean isCompatible(CustomParser p) {
    return p instanceof XlsParser;
  }
}

