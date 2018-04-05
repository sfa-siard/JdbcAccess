/*======================================================================
A header describing the columns of a ResultSet.
Application : Access JDBC driver
Description : ResultSetHeader class.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 07.11.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.access;

import java.util.*;

/*====================================================================*/
/** Header class describing the columns of a rather trivial ResultSet
 * representing a subset of the columns of a single table.
 * @author Hartwig Thomas
 */
public class ResultSetHeader
{
  /** description of a column of the result set */ 
  private class ResultSetColumn
  {
    /** name of column */
    private String _sName = null;
    /** @return name of column. */
    String getName() { return _sName; }
    /** java.sql.Types data type */
    private int _iType = java.sql.Types.OTHER;
    /** @return java.sql.Types data type */
    int getType() { return _iType; }
    /** precision of column type */
    private int _iPrecision = -1;
    /** @return precision of column type */
    int getPrecision() { return _iPrecision; }
    /** scale of column type */
    private int _iScale = -1;
    /** @return scale of column type */
    int getScale() { return _iScale; }

    /*------------------------------------------------------------------*/
    /** constructor
     * @param sName name of column.
     * @param iType java.sql.Types type of column.
     * @param iPrecision precision of column.
     * @param iScale scale of column.
     */
    ResultSetColumn(String sName, int iType, int iPrecision, int iScale)
    {
      _sName = sName;
      _iType = iType;
      _iScale = iScale;
      _iPrecision = iPrecision;
    } /* constructor ResultSetColumn */ 
    
    /*------------------------------------------------------------------*/
    /** constructor
     * @param sName name of column.
     * @param iType java.sql.Types type of column.
     * @param iSize size of column.
     */
    ResultSetColumn(String sName, int iType, int iSize)
    {
      _sName = sName;
      _iType = iType;
      _iPrecision = iSize;
    } /* constructor ResultSetColumn */ 
    
    /*------------------------------------------------------------------*/
    /** constructor
     * @param sName name of column.
     * @param iType java.sql.Types type of column.
     */
    ResultSetColumn(String sName, int iType)
    {
      _sName = sName;
      _iType = iType;
    } /* constructor ResultSetColumn */ 
    
  } /* class ResultSetColumn */
  
  /** schema name */
  private String _sSchemaName = null;
  /** @return schema name */
  public String getSchemaName() { return _sSchemaName; }
  /** table name */
  private String _sTableName = null;
  /** @return table name */ 
  public String getTableName() { return _sTableName; }
  /** list of (unquoted) column names */
  private List<ResultSetColumn> _listColumns = null;

  /*------------------------------------------------------------------*/
  /** constructs an empty column set of the given table.
   * @param sSchemaName schema name of table.
   * @param sTableName name of table.
   */
  public ResultSetHeader(String sSchemaName, String sTableName)
  {
    _sSchemaName = sSchemaName;
    _sTableName = sTableName;
    _listColumns = new ArrayList<ResultSetColumn>();
  } /* constructor ResultSetHeader */
  
  /*------------------------------------------------------------------*/
  /** Adds name and type of the next column.
   * @param sName column name (unquoted).
   * @param iType column type (one of java.sql.Types).
   * @param iScale scale of column.
   * @param iPrecision precision of column.
   */
  public void addColumn(String sName, int iType, int iScale, int iPrecision)
  {
    ResultSetColumn rs = new ResultSetColumn(sName, iType, iPrecision, iScale);
    _listColumns.add(rs);
  } /* addColumn */
  
  /*------------------------------------------------------------------*/
  /** Adds name and type of the next column.
   * @param sName column name (unquoted).
   * @param iType column type (one of java.sql.Types).
   * @param iSize size of column.
   */
  public void addColumn(String sName, int iType, int iSize)
  {
    ResultSetColumn rs = new ResultSetColumn(sName, iType, iSize);
    _listColumns.add(rs);
  } /* addColumn */
  
  /*------------------------------------------------------------------*/
  /** Adds name and type of the next column.
   * @param sName column name (unquoted).
   * @param iType column type (one of java.sql.Types).
   */
  public void addColumn(String sName, int iType)
  {
    ResultSetColumn rs = new ResultSetColumn(sName, iType);
    _listColumns.add(rs);
  } /* addColumn */
  
  /*------------------------------------------------------------------*/
  /** Returns number of columns in header.
   * @return number of columns
   */
  public int getColumns() { return _listColumns.size(); }
  
  /*------------------------------------------------------------------*/
  /** Gets name of a column.
   * @param iColumn column index (0-based)
   * @return column name.
   */
  public String getName(int iColumn)
  {
    return _listColumns.get(iColumn).getName();
  } /* getName */
  
  /*------------------------------------------------------------------*/
  /** Gets type of a column. 
   * @param iColumn column index (0-based)
   * @return column type (one of java.sql.Types).
   */
  public int getType(int iColumn)
  {
    return _listColumns.get(iColumn).getType();
  } /* getType */
  
  /*------------------------------------------------------------------*/
  /** Gets scale of a column. 
   * @param iColumn column index (0-based)
   * @return scale.
   */
  public int getScale(int iColumn)
  {
    return _listColumns.get(iColumn).getScale();
  } /* getScale */
  
  /*------------------------------------------------------------------*/
  /** Gets precision of a column. 
   * @param iColumn column index (0-based)
   * @return scale.
   */
  public int getPrecision(int iColumn)
  {
    return _listColumns.get(iColumn).getPrecision();
  } /* getPrecision */
  
} /* ResultSetHeader */
