/*======================================================================
AccessResultSet implements wrapped Jackcess ResultSet for MS Access.
Application : Access JDBC driver
Description : AccessResultSet implements wrapped Jackcess ResultSet for 
              MS Access.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 04.11.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.math.*;
import java.net.*;
import java.nio.charset.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import javax.xml.datatype.*;
import com.healthmarketscience.jackcess.Row;
import ch.enterag.utils.*;
import ch.enterag.sqlparser.*;
import ch.enterag.utils.jdbc.*;
import ch.admin.bar.siard2.access.*;
import ch.admin.bar.siard2.access.expression.*;

/*====================================================================*/
/** AccessResultSet implements wrapped Jackcess ResultSet for MS Access.
 * @author Hartwig Thomas
 */
public class AccessResultSet
  extends BaseResultSet
  implements ResultSet
{
  private static final int iBUFSIZ = 8192;
  /** connection */
  private Connection _conn = null;
  /** statement */ 
  private Statement _stmt = null;
  /** a header is an array of column names and types. */
  ResultSetHeader _header = null;
  /** @return result set header */
  public ResultSetHeader getHeader() { return _header; } 
  /** a cursor points to a current position */
  ResultSetCursor _cursor = null;
  /** @return result set cursor */
  public ResultSetCursor getCursor() { return _cursor; }
  /** a record is an array of columns. */
  private Row _row = null;
  /** there is a "current" row and an "insert" row */
  private Row _rowCurrent = null;
  private Row _rowInsert = null;
  /** fetch size is ignored */
  private int _iFetchSize = 1;
  /** fetch direction is ignored */ 
  private int _iFetchDirection = ResultSet.FETCH_UNKNOWN;
  /** record NULL value */
  private boolean _bWasNull = false;

  private byte[] readBytes(InputStream is, int iLength)
    throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[iBUFSIZ];
    int iToBeRead = iBUFSIZ;
    if (iLength < iToBeRead)
      iToBeRead = iLength;
    for (int iRead = is.read(buf,0,iToBeRead); (iToBeRead > 0) && (iRead != -1); iRead = is.read(buf,0,iToBeRead))
    {
      baos.write(buf,0,iRead);
      iLength -= iRead;
      if (iLength < iToBeRead)
        iToBeRead = iLength;
    }
    is.close();
    baos.close();
    return baos.toByteArray();
  } /* readBytes */
  
  private byte[] readBytes(InputStream is)
    throws IOException
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[iBUFSIZ];
    for (int iRead = is.read(buf); iRead != -1; iRead = is.read(buf))
      baos.write(buf,0,iRead);
    is.close();
    baos.close();
    return baos.toByteArray();
  } /* readBytes */
  
  private String readString(Reader rdr, int iLength)
    throws IOException
  {
    StringWriter sw = new StringWriter();
    char[] cbuf = new char[iBUFSIZ];
    int iToBeRead = iBUFSIZ;
    if (iLength < iToBeRead)
      iToBeRead = iLength;
    for (int iRead = rdr.read(cbuf,0,iToBeRead); (iToBeRead > 0) && (iRead != -1); iRead = rdr.read(cbuf,0,iToBeRead))
    {
      sw.write(cbuf,0,iRead);
      iLength -= iRead;
      if (iLength < iToBeRead)
        iToBeRead = iLength;
    }
    rdr.close();
    sw.close();
    return sw.toString();
  } /* readString */

  private String readString(Reader rdr)
    throws IOException
  {
    StringWriter sw = new StringWriter();
    char[] cbuf = new char[iBUFSIZ];
    for (int iRead = rdr.read(cbuf); iRead != -1; iRead = rdr.read(cbuf))
      sw.write(cbuf,0,iRead);
    rdr.close();
    sw.close();
    return sw.toString();
  } /* readString */
  /*------------------------------------------------------------------*/
  /** constructor with a header and a cursor.
   * @param conn Connection object.
   * @param stmt Statement object.
   * @param header list of column names and types.
   * @param cursor list of records accessed by row.
   */
  AccessResultSet(Connection conn, Statement stmt, ResultSetHeader header, ResultSetCursor cursor)
  {
    super(null);
    _conn = conn;
    _stmt = stmt;
    _header = header;
    _cursor = cursor;
  } /* constructor AccessResultSet */
  
  /*======================================================================
  Wrapper 
  ======================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean isWrapperFor(Class<?> clsInterface) throws SQLException
  {
    return clsInterface.equals(ResultSet.class);
  } /* isWrapperFor */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clsInterface) throws SQLException
  {
    T impl = null;
    if (isWrapperFor(clsInterface))
      impl = (T)this;
    else
      throw new IllegalArgumentException("AccessResultSet cannot be unwrapped to "+clsInterface.getName()+"!");
    return impl;
  } /* unwrap */

  /*======================================================================
  Navigation 
  ======================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public int getRow() throws SQLException
  {
    return _cursor.getRow();
  } /* getRow */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean isAfterLast() throws SQLException
  {
    boolean bAfterLast = false;
    try { bAfterLast = _cursor.isAfterLast(); }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return bAfterLast;
  } /* isAfterLast */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    boolean bBeforeFirst = false;
    try { bBeforeFirst = _cursor.isBeforeFirst(); }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return bBeforeFirst;
  } /* isBeforeFirst */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean isFirst() throws SQLException
  {
    boolean bFirst = false;
    try
    {
      if (_row != null)
      {
        Row row = _cursor.getPreviousRow();
        if (row == null)
          bFirst = true;
        _cursor.getNextRow();
      }
    }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return bFirst;
  } /* isFirst */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean isLast() throws SQLException
  {
    boolean bLast = false;
    try
    {
      if (_row != null)
      {
        Row row = _cursor.getNextRow();
        if (row == null)
          bLast = true;
        _cursor.getPreviousRow();
      }
    }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return bLast;
  } /* isLast */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void afterLast() throws SQLException
  {
    try { _cursor.afterLast(); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* afterLast */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void beforeFirst() throws SQLException
  {
    _cursor.beforeFirst();
  } /* beforeFirst */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean first() throws SQLException
  {
    _cursor.beforeFirst();
    try { _row = _cursor.getNextRow(); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return (_row != null);
  } /* first */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean last() throws SQLException
  {
    try 
    { 
      _cursor.afterLast();
      _row = _cursor.getPreviousRow(); 
    }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return (_row != null);
  } /* last */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean absolute(int iRow) throws SQLException
  {
    boolean bPositioned = false;
    /* move to before first */
    _cursor.beforeFirst();
    try
    {
      int i = 1;
      if (iRow > 0)
      {
        /* iRow = 1 is first row! */
        for (_row = _cursor.getNextRow(); (_row != null) && (i < iRow); _row = _cursor.getNextRow())
          i++;
      }
      else if (iRow < 0)
      {
        /* iRow = -1 is last row */
        _cursor.afterLast();
        for (_row = _cursor.getPreviousRow(); (_row != null) && (i > iRow); _row = _cursor.getPreviousRow())
          i--;
      }
      if (i == iRow)
        bPositioned = true;
    }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return bPositioned;
  } /* absolute */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean relative(int iRow) throws SQLException
  {
    int iAbsolute = getRow()+iRow;
    return absolute(iAbsolute);
  } /* relative */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean next() throws SQLException
  {
    try { _row = _cursor.getNextRow(); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return (_row != null);
  } /* next */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean previous() throws SQLException
  {
    try { _row = _cursor.getPreviousRow(); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return (_row != null);
  } /* previous */

  /*======================================================================
  Row updating 
  ======================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void deleteRow() throws SQLException
  {
    try { _cursor.deleteCurrentRow(); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* deleteRow */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void insertRow() throws SQLException
  {
    try { _cursor.insertRow(_row); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* insertRow */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNull(String sColumnLabel) throws SQLException
  {
    _row.put(sColumnLabel, null);
  } /* updateNull */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNull(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateNull(sColumnLabel);
  } /* updateNull */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateObject(String sColumnLabel, Object x)
      throws SQLException
  {
    if (x instanceof BigDecimal)
      updateBigDecimal(sColumnLabel, (BigDecimal)x);
    else if (x instanceof Date)
      updateDate(sColumnLabel,(Date)x);
    else if (x instanceof Time)
      updateTime(sColumnLabel, (Time)x);
    else if (x instanceof Duration)
      updateDuration(sColumnLabel, (Duration)x);
    else if (x instanceof NClob)
      updateNClob(sColumnLabel, (NClob)x);
    else if (x instanceof Clob)
      updateClob(sColumnLabel, (Clob)x);
    else if (x instanceof SQLXML)
      updateSQLXML(sColumnLabel,(SQLXML)x);
    else if (x instanceof Blob)
      updateBlob(sColumnLabel, (Blob)x);
    else if (x instanceof InputStream)
      updateBinaryStream(sColumnLabel, (InputStream)x);
    else if (x instanceof Reader)
      updateCharacterStream(sColumnLabel, (Reader)x);
    else if (x instanceof URL)
      updateURL(sColumnLabel, (URL) x);
    else
      _row.put(sColumnLabel, x);
  } /* updateObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateObject(int iColumnIndex, Object x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateObject(sColumnLabel, x);
  } /* updateObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateObject(String sColumnLabel, Object x, int iScaleOrLength)
      throws SQLException
  {
    // TODO: according to type and scale or length of x call specific updates
    _row.put(sColumnLabel, x);
  } /* updateObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateObject(int iColumnIndex, Object x, int iScaleOrLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateObject(sColumnLabel, x, iScaleOrLength);
  } /* updateObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBigDecimal(String sColumnLabel, BigDecimal x)
      throws SQLException
  {
    _row.put(sColumnLabel, x.stripTrailingZeros());
  } /* updateBigDecimal */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBigDecimal(int iColumnIndex, BigDecimal x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBigDecimal(sColumnLabel, x);
  } /* updateBigDecimal */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBoolean(String sColumnLabel, boolean x)
      throws SQLException
  {
    _row.put(sColumnLabel, Boolean.valueOf(x));
  } /* updateBoolean */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBoolean(int iColumnIndex, boolean x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBoolean(sColumnLabel, x);
  } /* updateBoolean */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateByte(String sColumnLabel, byte x) throws SQLException
  {
    _row.put(sColumnLabel, Byte.valueOf(x));
  } /* updateByte */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateByte(int iColumnIndex, byte x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateByte(sColumnLabel, x);    
  } /* updateByte */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateDouble(String sColumnLabel, double x)
      throws SQLException
  {
    _row.put(sColumnLabel, Double.valueOf(x));
  } /* updateDouble */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateDouble(int iColumnIndex, double x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateDouble(sColumnLabel, x);    
  } /* updateDouble */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateFloat(String sColumnLabel, float x) throws SQLException
  {
    _row.put(sColumnLabel, Float.valueOf(x));
  } /* updateFloat */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateFloat(int iColumnIndex, float x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateFloat(sColumnLabel, x);    
  } /* updateFloat */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateInt(String sColumnLabel, int x) throws SQLException
  {
    _row.put(sColumnLabel, Integer.valueOf(x));
  } /* updateInt */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateInt(int iColumnIndex, int x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateInt(sColumnLabel, x);    
  } /* updateInt */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateShort(String sColumnLabel, short x) throws SQLException
  {
    _row.put(sColumnLabel, Short.valueOf(x));
  } /* updateShort */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateShort(int iColumnIndex, short x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateShort(sColumnLabel, x);    
  } /* updateShort */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateLong(String sColumnLabel, long x) throws SQLException
  {
    _row.put(sColumnLabel, BigDecimal.valueOf(x));
  } /* updateLong */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateLong(int iColumnIndex, long x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateLong(sColumnLabel, x);    
  } /* updateLong */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateString(String sColumnLabel, String x)
      throws SQLException
  {
    _row.put(sColumnLabel, x);
  } /* updateString */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateString(int iColumnIndex, String x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateString(sColumnLabel, x);    
  } /* updateString */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNString(String sColumnLabel, String x)
      throws SQLException
  {
    updateString(sColumnLabel, x);
  } /* updateNString */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNString(int iColumnIndex, String x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateNString(sColumnLabel, x);
  } /* updateNString */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateDate(String sColumnLabel, Date x) throws SQLException
  {
    Timestamp ts = new Timestamp(x.getTime());
    _row.put(sColumnLabel, ts);
  } /* updateDate */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateDate(int iColumnIndex, Date x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateDate(sColumnLabel, x);
  } /* updateDate */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateTime(String sColumnLabel, Time x) throws SQLException
  {
    Timestamp ts = new Timestamp(x.getTime());
    _row.put(sColumnLabel, ts);
  } /* updateTime */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateTime(int iColumnIndex, Time x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateTime(sColumnLabel, x);
  } /* updateTime */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateTimestamp(String sColumnLabel, Timestamp x)
      throws SQLException
  {
    _row.put(sColumnLabel, x);
  } /* updateTimestamp */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateTimestamp(int iColumnIndex, Timestamp x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateTimestamp(sColumnLabel, x);
  } /* updateTimestamp */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  public void updateDuration(String sColumnLabel, Duration x)
      throws SQLException
  {
    Interval iv = Interval.fromDuration(x);
    BigDecimal bd = AccessUnsignedLiteral.convertInterval(iv);
    _row.put(sColumnLabel, bd);
  } /* updateDuration */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  public void updateDuration(int iColumnIndex, Duration x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateDuration(sColumnLabel, x);
  } /* updateDuration */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBytes(String sColumnLabel, byte[] x) throws SQLException
  {
    _row.put(sColumnLabel, x);
  } /* updateBytes */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBytes(int iColumnIndex, byte[] x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBytes(sColumnLabel, x);
  } /* updateBytes */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBlob(String sColumnLabel, Blob x) throws SQLException
  {
    _row.put(sColumnLabel, x.getBytes(1l,(int)x.length()));
  } /* updateBlob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBlob(int iColumnIndex, Blob x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBlob(sColumnLabel, x);    
  } /* updateBlob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBlob(String sColumnLabel, InputStream x)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readBytes(x)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateBlob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBlob(int iColumnIndex, InputStream x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBlob(sColumnLabel, x);
  } /* updateBlob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBlob(String sColumnLabel, InputStream x, long lLength)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readBytes(x, (int)lLength)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateBlob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBlob(int iColumnIndex, InputStream x, long lLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBlob(sColumnLabel, x, lLength);    
  } /* updateBlob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateClob(String sColumnLabel, Clob x) throws SQLException
  {
    _row.put(sColumnLabel, x.getSubString(1l, (int)x.length()));
  } /* updateClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateClob(int iColumnIndex, Clob x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateClob(sColumnLabel, x);    
  } /* updateClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateClob(String sColumnLabel, Reader x) throws SQLException
  {
    try { _row.put(sColumnLabel, readString(x)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateClob(int iColumnIndex, Reader x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateClob(sColumnLabel, x);    
  } /* updateClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateClob(String sColumnLabel, Reader x, long lLength)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readString(x, (int)lLength)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateClob(int iColumnIndex, Reader x, long lLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateClob(sColumnLabel, x, lLength);    
  } /* updateClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNClob(String sColumnLabel, NClob x) throws SQLException
  {
    updateClob(sColumnLabel, x);
  } /* updateNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNClob(int iColumnIndex, NClob x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateNClob(sColumnLabel, x);
  } /* updateNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNClob(String sColumnLabel, Reader x) throws SQLException
  {
    updateClob(sColumnLabel, x);
  } /* updateNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNClob(int iColumnIndex, Reader x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateNClob(sColumnLabel, x);
  } /* updateNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNClob(String sColumnLabel, Reader x, long lLength)
      throws SQLException
  {
    updateClob(sColumnLabel, x, lLength);
  } /* updateNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNClob(int iColumnIndex, Reader x, long lLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateNClob(sColumnLabel, x, lLength);
  } /* updateNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateRef(String sColumnLabel, Ref x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Row updates are not supported!");
  } /* updateRef */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateRef(int iColumnIndex, Ref x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateRef(sColumnLabel, x);
  } /* updateRef */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateRowId(String sColumnLabel, RowId x) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Row updates are not supported!");
  } /* updateRowId */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateRowId(int iColumnIndex, RowId x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateRowId(sColumnLabel, x);    
  } /* updateRowId */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateArray(String sColumnLabel, Array x) throws SQLException
  {
    Object[] ao = (Object[])x.getArray();
    List<Object> list = Arrays.asList(ao);
    _row.put(sColumnLabel, list);
  } /* updateArray */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateArray(int iColumnIndex, Array x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateArray(sColumnLabel, x);
  } /* updateArray */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateSQLXML(String sColumnLabel, SQLXML x)
      throws SQLException
  {
    _row.put(sColumnLabel, x.getString());
  } /* updateSQLXML */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateSQLXML(int iColumnIndex, SQLXML x) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateSQLXML(sColumnLabel, x);
  } /* updateSQLXML */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBinaryStream(String sColumnLabel, InputStream x)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readBytes(x)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBinaryStream(int iColumnIndex, InputStream x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBinaryStream(sColumnLabel, x);
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBinaryStream(String sColumnLabel, InputStream x, int iLength)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readBytes(x, iLength)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBinaryStream(int iColumnIndex, InputStream x, int iLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBinaryStream(sColumnLabel, x, iLength);
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBinaryStream(String sColumnLabel, InputStream x, long lLength)
    throws SQLException
  {
    try { _row.put(sColumnLabel, readBytes(x, (int)lLength)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateBinaryStream(int iColumnIndex, InputStream x, long lLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateBinaryStream(sColumnLabel, x, lLength);
  } /* updateBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateAsciiStream(String sColumnLabel, InputStream x)
      throws SQLException
  {
    try { _row.put(sColumnLabel, new String(readBytes(x),StandardCharsets.US_ASCII)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateAsciiStream(int iColumnIndex, InputStream x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateAsciiStream(sColumnLabel, x);
  } /* updateAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateAsciiStream(String sColumnLabel, InputStream x, int iLength)
      throws SQLException
  {
    try { _row.put(sColumnLabel, new String(readBytes(x,iLength),StandardCharsets.US_ASCII)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateAsciiStream(int iColumnIndex, InputStream x, int iLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateAsciiStream(sColumnLabel, x, iLength);
  } /* updateAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateAsciiStream(String sColumnLabel, InputStream x, long lLength)
      throws SQLException
  {
    try { _row.put(sColumnLabel, new String(readBytes(x,(int)lLength),StandardCharsets.US_ASCII)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateAsciiStream(int iColumnIndex, InputStream x, long lLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateAsciiStream(sColumnLabel, x, lLength);
  } /* updateAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateCharacterStream(String sColumnLabel, Reader x)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readString(x)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateCharacterStream(int iColumnIndex, Reader x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateCharacterStream(sColumnLabel, x);
  } /* updateCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateCharacterStream(String sColumnLabel, Reader x, int iLength)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readString(x, iLength)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateCharacterStream(int iColumnIndex, Reader x, int iLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateCharacterStream(sColumnLabel, x, iLength);
  } /* updateCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateCharacterStream(String sColumnLabel, Reader x, long lLength)
      throws SQLException
  {
    try { _row.put(sColumnLabel, readString(x, (int)lLength)); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateCharacterStream */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateCharacterStream(int iColumnIndex, Reader x, long lLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateCharacterStream(sColumnLabel, x, lLength);
  } /* updateCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNCharacterStream(String sColumnLabel, Reader x)
      throws SQLException
  {
    updateCharacterStream(sColumnLabel, x);
  } /* updateNCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNCharacterStream(int iColumnIndex, Reader x)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateNCharacterStream(sColumnLabel, x);
  } /* updateNCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNCharacterStream(String sColumnLabel, Reader x, long lLength)
      throws SQLException
  {
    updateCharacterStream(sColumnLabel, x, lLength);
  } /* updateNCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateNCharacterStream(int iColumnIndex, Reader x, long lLength)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    updateNCharacterStream(sColumnLabel, x, lLength);
  } /* updateNCharacterStream */

  public URL updateURL(int columnIndex, URL url) throws SQLException {
    super.updateObject(columnIndex, url.getPath());
    return url;
  }

  public URL updateURL(String columnLabel, URL url) throws SQLException {
    updateURL(this.findColumn(columnLabel), url);
    return url;
  }

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void updateRow() throws SQLException
  {
    try { _cursor.updateCurrentRow(_row); }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* updateRow */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean rowDeleted() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Detection of deleted rows is not supported!");
  } /* rowDeleted */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean rowInserted() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Detection of inserted rows is not supported!");
  } /* rowInserted */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean rowUpdated() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Detection of updated rows is not supported!");
  } /* rowUpdated */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void cancelRowUpdates() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Row updates are not supported in Access JDBC!");
  } /* cancelRowUpdates */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void moveToCurrentRow() throws SQLException
  {
    if (_row == _rowInsert)
    {
      _row = _rowCurrent;
      _rowInsert = null;
    }
  } /* moveToCurrentRow */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void moveToInsertRow() throws SQLException
  {
    if ((_rowInsert == null) || (_row != _rowInsert))
    {
      _rowCurrent = _row;
      _rowInsert = new ResultSetRow();
      ResultSetMetaData rsmd = getMetaData();
      for (int iColumn = 0; iColumn < rsmd.getColumnCount(); iColumn++)
      {
        String sColumnName = rsmd.getColumnLabel(iColumn+1);
        _rowInsert.put(sColumnName, null); // all values are null initially
      }
      _row = _rowInsert;
    }
  } /* moveToInsertRow */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void refreshRow() throws SQLException
  {
    try { _row = _cursor.refreshCurrentRow(); }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* refreshRow */
  
  /*======================================================================
  ResultSet state 
  ======================================================================*/
  /*------------------------------------------------------------------*/
  /** Only read-only cursors are supported. 
   * {@link ResultSet} 
   */
  @Override
  public int getConcurrency() throws SQLException
  {
    return ResultSet.CONCUR_READ_ONLY;
  } /* getConcurrency */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public int getFetchDirection() throws SQLException
  {
    return _iFetchDirection;
  } /* getFetchDirection */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void setFetchDirection(int iFetchDirection) throws SQLException
  {
    _iFetchDirection = iFetchDirection;
  } /* setFetchDirection */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public int getFetchSize() throws SQLException
  {
    return _iFetchSize;
  } /* getFetchSize */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void setFetchSize(int iFetchSize) throws SQLException
  {
    _iFetchSize = iFetchSize;
  } /* setFetchSize */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} 
   * We do not handle commit ... */
  @Override
  public int getHoldability() throws SQLException
  {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  } /* getHoldability */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet}
   * This is not 100% clear. But we mainly use JDBC ResultSets in a
   * read-only context. 
   */
  @Override
  public int getType() throws SQLException
  {
    return ResultSet.TYPE_SCROLL_INSENSITIVE;
  } /* getType */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public String getCursorName() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Cursors are not supported in Access JDBC!");
  } /* getCursorName */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Statement getStatement() throws SQLException
  {
    return _stmt;
  } /* getStatement */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    return new AccessResultSetMetaData(getHeader(),_conn);
  } /* getMetaData */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void clearWarnings() throws SQLException
  {
    _conn.clearWarnings();
  } /* clearWarnings */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    return _conn.getWarnings();
  } /* getWarnings */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean isClosed() throws SQLException
  {
    return (_cursor == null);
  } /* isClosed */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public void close() throws SQLException
  {
    _conn = null;
    _stmt = null;
    _header = null;
    _cursor = null;
  } /* close */

  /*======================================================================
  Column access 
  ======================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public int findColumn(String sColumnLabel) throws SQLException
  {
    int iColumn = -1;
    for (iColumn = 0; (!_header.getName(iColumn).equals(sColumnLabel)) && (iColumn < _header.getColumns()); iColumn++) {}
    if (!_header.getName(iColumn).equals(sColumnLabel))
      throw new SQLException("ResultSet does not contain a column labeled "+sColumnLabel);
    /* column indices are 1-based! */
    return iColumn+1;
  } /* findColumn */

  /*======================================================================
  Column access through types 
  ======================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public BigDecimal getBigDecimal(String sColumnLabel) throws SQLException
  {
    BigDecimal bd = Conversions.getBigDecimal(_row.get(sColumnLabel));
    _bWasNull = false;
    if (bd == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return bd;
  } /* getBigDecimal */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public BigDecimal getBigDecimal(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getBigDecimal(sColumnLabel);
  } /* getBigDecimal */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public BigDecimal getBigDecimal(String sColumnLabel, int iScale)
      throws SQLException
  {
    BigDecimal bd = Conversions.getBigDecimal(_row.get(sColumnLabel), iScale);
    bd = bd.setScale(iScale, RoundingMode.DOWN);
    return bd;
  } /* getBigDecimal */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public BigDecimal getBigDecimal(int iColumnIndex, int iScale)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getBigDecimal(sColumnLabel,iScale);
  } /* getBigDecimal */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean getBoolean(String sColumnLabel) throws SQLException
  {
    Boolean b = Conversions.getBoolean(_row.get(sColumnLabel));
    _bWasNull = false;
    if (b == null)
    {
      b = Boolean.valueOf(false);
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return b.booleanValue(); 
  } /* getBoolean */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean getBoolean(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getBoolean(sColumnLabel);
  } /* getBoolean */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public byte getByte(String sColumnLabel) throws SQLException
  {
    Byte b = Conversions.getByte(_row.get(sColumnLabel));
    _bWasNull = false;
    if (b == null)
    {
      b = Byte.valueOf((byte)0);
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return b.byteValue();
  } /* getByte */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public byte getByte(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getByte(sColumnLabel);
  }

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public double getDouble(String sColumnLabel) throws SQLException
  {
    Double d = Conversions.getDouble(_row.get(sColumnLabel));
    _bWasNull = false;
    if (d == null)
    {
      d = Double.valueOf(0.0);
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return d.doubleValue();
  } /* getDouble */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public double getDouble(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getDouble(sColumnLabel);
  } /* getDouble */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public float getFloat(String sColumnLabel) throws SQLException
  {
    Float f = Conversions.getFloat(_row.get(sColumnLabel));
    _bWasNull = false;
    if (f == null)
    {
      f = Float.valueOf(0.0f);
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return f.floatValue();
  } /* getFloat */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public float getFloat(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getFloat(sColumnLabel);
  } /* getFloat */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public int getInt(String sColumnLabel) throws SQLException
  {
    Integer i = Conversions.getInt(_row.get(sColumnLabel));
    _bWasNull = false;
    if (i == null)
    {
      i = Integer.valueOf(0);
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return i.intValue();
  } /* getInt */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public int getInt(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getInt(sColumnLabel);
  } /* getInt */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public short getShort(String sColumnLabel) throws SQLException
  {
    Short w = Conversions.getShort(_row.get(sColumnLabel));
    _bWasNull = false;
    if (w == null)
    {
      w = Short.valueOf((short)0);
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return w.shortValue();
  } /* getShort */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public short getShort(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getShort(sColumnLabel);
  } /* getShort */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public long getLong(String sColumnLabel) throws SQLException
  {
    Long l = Conversions.getLong(_row.get(sColumnLabel));
    _bWasNull = false;
    if (l == null)
    {
      l = Long.valueOf(0l);
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return l.longValue();
  } /* getLong */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public long getLong(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getLong(sColumnLabel);
  } /* getLong */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public String getString(String sColumnLabel) throws SQLException
  {
    String s = Conversions.getString(_row.get(sColumnLabel));
    _bWasNull = false;
    if (s == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return s;
  } /* getString */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public String getString(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getString(sColumnLabel);
  } /* sColumnLabel */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public String getNString(String sColumnLabel) throws SQLException
  {
    return getString(sColumnLabel);
  } /* getNString */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public String getNString(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getNString(sColumnLabel);
  } /* getNString */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Date getDate(String sColumnLabel) throws SQLException
  {
    java.sql.Date date = Conversions.getDate(_row.get(sColumnLabel));
    _bWasNull = false;
    if (date == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return date;
  } /* getDate */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Date getDate(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getDate(sColumnLabel);
  } /* getDate */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet}
   * It does not become clear from the description, what the function
   * of cal is ... 
   */
  @Override
  public Date getDate(String sColumnLabel, Calendar cal) throws SQLException
  {
    return getDate(sColumnLabel);
  } /* getDate */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Date getDate(int iColumnIndex, Calendar cal) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getDate(sColumnLabel,cal);
  } /* getDate */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Time getTime(String sColumnLabel) throws SQLException
  {
    java.sql.Time time = Conversions.getTime(_row.get(sColumnLabel));
    _bWasNull = false;
    if (time == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return time;
  } /* getTime */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Time getTime(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getTime(sColumnLabel);
  } /* getTime */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet}
   * It does not become clear from the description, what the function
   * of cal is ... 
   */
  @Override
  public Time getTime(String sColumnLabel, Calendar cal) throws SQLException
  {
    return getTime(sColumnLabel);
  } /* getTime */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Time getTime(int iColumnIndex, Calendar cal) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getTime(sColumnLabel,cal);
  } /* getTime */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Timestamp getTimestamp(String sColumnLabel) throws SQLException
  {
    java.sql.Timestamp ts = Conversions.getTimestamp(_row.get(sColumnLabel));
    _bWasNull = false;
    if (ts == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return ts;
  } /* getTimestamp */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  public Duration getDuration(String sColumnLabel) throws SQLException
  {
    Duration duration = Conversions.getDuration(_row.get(sColumnLabel));
    _bWasNull = false;
    if (duration == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return duration;
  } /* getDuration */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Timestamp getTimestamp(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getTimestamp(sColumnLabel);
  } /* getTimestamp */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet}
   * It does not become clear from the description, what the function
   * of cal is ... 
   */
  @Override
  public Timestamp getTimestamp(String sColumnLabel, Calendar cal)
      throws SQLException
  {
    return getTimestamp(sColumnLabel);
  } /* getTimestamp */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Timestamp getTimestamp(int iColumnIndex, Calendar cal)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getTimestamp(sColumnLabel,cal);
  } /* getTimestamp */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public byte[] getBytes(String sColumnLabel) throws SQLException
  {
    byte[] buf = Conversions.getBytes(_row.get(sColumnLabel));
    _bWasNull = false;
    if (buf == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return buf;
  } /* getBytes */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public byte[] getBytes(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getBytes(sColumnLabel);
  } /* getBytes */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Blob getBlob(String sColumnLabel) throws SQLException
  {
    Blob blob = Conversions.getBlob(_row.get(sColumnLabel));
    _bWasNull = false;
    if (blob == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return blob;
  } /* getBlob */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Blob getBlob(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getBlob(sColumnLabel);
  } /* getBlob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Clob getClob(String sColumnLabel) throws SQLException
  {
    Clob clob = Conversions.getClob(_row.get(sColumnLabel));
    _bWasNull = false;
    if (clob == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return clob;
  } /* getClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Clob getClob(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getClob(sColumnLabel);
  } /* getClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public NClob getNClob(String sColumnLabel) throws SQLException
  {
    NClob nclob = Conversions.getNClob(_row.get(sColumnLabel));
    _bWasNull = false;
    if (nclob == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return nclob;
  } /* getNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public NClob getNClob(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getNClob(sColumnLabel);
  } /* getNClob */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public SQLXML getSQLXML(String sColumnLabel) throws SQLException
  {
    SQLXML sx = Conversions.getSqlXml(_row.get(sColumnLabel));
    _bWasNull = false;
    if (sx == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return sx;
  } /* getSQLXML */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public SQLXML getSQLXML(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getSQLXML(sColumnLabel);
  } /* getSQLXML */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public URL getURL(String sColumnLabel) throws SQLException
  {
    URL url = Conversions.getURL(_row.get(sColumnLabel));
    _bWasNull = false;
    if (url == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return url;
  } /* getURL */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public URL getURL(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getURL(sColumnLabel);
  } /* getURL */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public InputStream getBinaryStream(String sColumnLabel) throws SQLException
  {
    ByteArrayInputStream bais = null; 
    byte[] buf = Conversions.getBytes(_row.get(sColumnLabel));
    _bWasNull = false;
    if (buf == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    else
      bais = new ByteArrayInputStream(buf); 
    return bais;
  } /* getBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public InputStream getBinaryStream(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getBinaryStream(sColumnLabel);
  } /* getBinaryStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public InputStream getAsciiStream(String sColumnLabel) throws SQLException
  {
    /* ASCII stream is a stream of bytes - just like binary stream */
    ByteArrayInputStream bais = null; 
    String s = Conversions.getString(_row.get(sColumnLabel));
    _bWasNull = false;
    if (s == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    else
      bais = new ByteArrayInputStream(s.getBytes()); 
    return bais;
  } /* getAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public InputStream getAsciiStream(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getAsciiStream(sColumnLabel);
  } /* getAsciiStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Reader getCharacterStream(String sColumnLabel) throws SQLException
  {
    StringReader rdr = null;
    String s = Conversions.getString(_row.get(sColumnLabel));
    _bWasNull = false;
    if (s == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    else
      rdr = new StringReader(s); 
    return rdr;
  } /* getCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Reader getCharacterStream(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getCharacterStream(sColumnLabel);
  } /* getCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Reader getNCharacterStream(String sColumnLabel) throws SQLException
  {
    return getCharacterStream(sColumnLabel);
  } /* getNCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Reader getNCharacterStream(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getNCharacterStream(sColumnLabel);
  } /* getNCharacterStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public InputStream getUnicodeStream(String sColumnLabel) throws SQLException
  {
    ByteArrayInputStream bais = null;
    String s = Conversions.getString(_row.get(sColumnLabel));
    _bWasNull = false;
    if (s == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    else
      bais = new ByteArrayInputStream(SU.putEncodedString(s, "UTF-16"));
    return bais;
  } /* getUnicodeStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public InputStream getUnicodeStream(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getUnicodeStream(sColumnLabel);
  } /* getUnicodeStream */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Object getObject(String sColumnLabel) throws SQLException
  {
    Object o = _row.get(sColumnLabel);
    _bWasNull = false;
    if (o == null)
    {
      if (_row.containsKey(sColumnLabel))
        _bWasNull = true;
    }
    return o;    
  } /* getObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Object getObject(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getObject(sColumnLabel);
  } /* getObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Object getObject(String sColumnLabel, Map<String, Class<?>> mapUdt)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("UDTs are not supported in Access JDBC!");
  } /* getObject */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Object getObject(int iColumnIndex, Map<String, Class<?>> mapUdt)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getObject(sColumnLabel,mapUdt);
  } /* getObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} for JDK 1.7 */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getObject(String sColumnLabel, Class<T> type)
      throws SQLException
  {
    return (T)getObject(sColumnLabel);
  } /* getObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} for JDK 1.7 */
  @Override
  public <T> T getObject(int iColumnIndex, Class<T> type)
      throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getObject(sColumnLabel,type);
  } /* getObject */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Array getArray(String sColumnLabel) throws SQLException
  {
    return (Array)_row.get(sColumnLabel);
  } /* getArray */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Array getArray(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getArray(sColumnLabel);
  } /* getArray */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Ref getRef(String sColumnLabel) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Refs are not supported in Access JDBC!");
  } /* getRef */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public Ref getRef(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getRef(sColumnLabel);
  }

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public RowId getRowId(String sColumnLabel) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("RowIds are not supported in Access JDBC!");
  } /* getRowId */

  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public RowId getRowId(int iColumnIndex) throws SQLException
  {
    String sColumnLabel = _header.getName(iColumnIndex-1);
    return getRowId(sColumnLabel);
  } /* getRowId */
  
  /*------------------------------------------------------------------*/
  /** {@link ResultSet} */
  @Override
  public boolean wasNull() throws SQLException
  {
    return _bWasNull;
  } /* wasNull */

} /* AccessResultSet */
