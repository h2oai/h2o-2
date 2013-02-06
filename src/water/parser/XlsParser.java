package water.parser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.poi.hssf.eventusermodel.*;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.record.*;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import water.DKV;
import water.Key;
import water.parser.ValueString;

public class XlsParser extends CustomParser implements HSSFListener {

  private POIFSFileSystem _fs;
  private final DParseTask _callback;
  private FormatTrackingHSSFListener _formatListener;
  
  private final ValueString _str = new ValueString();

  public XlsParser(DParseTask callback) throws IOException {
    _callback = callback;
  }

  @Override public void parse(Key key) throws IOException {
    _firstRow = true;
    InputStream is = DKV.get(key).openStream();
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
  }
  
  ArrayList<String> _columnNames = new ArrayList();
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
          System.err.println("[ExcelParser] Missing SST record");
        } else {
          curCol = lsrec.getColumn();
          curStr = _str.setTo(_sstRecord.getString(lsrec.getSSTIndex()).toString());
        }
        break;
      case NoteRecord.sid:
        System.err.println("[ExcelParser] Warning cell notes are unsupported");
        break;
      case NumberRecord.sid:
        NumberRecord numrec = (NumberRecord) record;
        curCol = numrec.getColumn();
        curNum = numrec.getValue();
        break;
      case RKRecord.sid:
        System.err.println("[ExcelParser] Warning RK records are unsupported");
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
        _callback.setColumnNames(arr);
      }
      _callback.newLine();
    }

    if (curCol == -1)
      return;
    
    if (_firstRow) {
      _columnNames.add(curStr == null ? "" : curStr.toString());
    } else {
      if (curStr == null)
        if (Double.isNaN(curNum))
          _callback.addInvalidCol(curCol);
        else
          _callback.addCol(curCol, curNum);
      else 
        _callback.addStrCol(curCol, curStr);
    }
  }

  private SSTRecord _sstRecord;
  private int _nextCol;
  private boolean _outputNextStringRecord;
}

