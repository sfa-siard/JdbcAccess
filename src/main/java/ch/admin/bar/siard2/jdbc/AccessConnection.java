/*======================================================================
AccessConnection implements a wrapped Jackcess Connection for MS Access.
Application : Access JDBC driver
Description : AccessConnection implements a wrapped Jackcess Connection 
              for MS Access.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 04.11.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import ch.enterag.utils.jdbc.*;
import com.healthmarketscience.jackcess.*;
import ch.enterag.sqlparser.*;
import ch.admin.bar.siard2.access.*;

/*====================================================================*/
/** AccessConnection implements a wrapped Jackcess Connection for MS Access.
 * @author Hartwig Thomas
 */
public class AccessConnection
  extends BaseConnection
  implements Connection
{
  /** last SQL warning */
  private SQLWarning _sw = null;
  /** Jackcess database */
  private Database _db = null;
  /** @return Jackcess database */
  Database getDatabase() { return _db; }
  /** user name used for connecting to Jackcess database */
  private String _sUserName = null;
  /** @return user name */
  String getUserName() { return _sUserName; }
  /** password used for connecting to Jackcess database */
  @SuppressWarnings("unused")
  private String _sPassword = null;
  /** log writer */
  private PrintWriter _pwLogWriter = null;
  /** @return log writer */
  PrintWriter getLogWriter() { return _pwLogWriter; }
  /** schema */
  private String _schema = null;
  /** read-only flag */
  private boolean _bReadOnly = false;
  
  /*====================================================================
  Wrapper 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public boolean isWrapperFor(Class<?> clsInterface) throws SQLException
  {
    return clsInterface.equals(Connection.class);
  } /* isWrapperFor */
  
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clsInterface) throws SQLException
  {
    T impl = null;
    if (isWrapperFor(clsInterface))
      impl = (T)this;
    else
      throw new IllegalArgumentException("AccessConnection cannot be unwrapped to "+clsInterface.getName()+"!");
    return impl;
  } /* unwrap */

  /*====================================================================
  Warnings 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void clearWarnings() throws SQLException
  {
    _sw = null;
  } /* clearWarnings */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    return _sw;
  } /* getWarnings */
  
  /*====================================================================
  Connection 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** constructor opens the MS Access database 
   * @param sDatabaseName file path of MDB or ACCDB.
   * @param sUserName user name for connecting to database.
   * @param sPassword password for connecting to database.
   * @param bReadOnly true, if database is to be opened read-only.
   * @param pwLogWriter a print writer to write messages to.
   * @throws SQLException if the file could not be opened.
   */
  public AccessConnection(String sDatabaseName, String sUserName, String sPassword, boolean bReadOnly, PrintWriter pwLogWriter)
    throws SQLException
  {
    super(null);
    _pwLogWriter = pwLogWriter;
    _bReadOnly = bReadOnly;
    _sUserName = sUserName;
    _schema = _sUserName;
    _sPassword = sPassword;
    try 
    { 
      /* suppress Jackcess warnings */
      Logger logger = Logger.getLogger("com.healthmarketscience.jackcess");
      logger.setLevel(Level.OFF);
      File fileDatabase = new File(sDatabaseName);
      if (fileDatabase.exists())
        _db = new DatabaseBuilder().
          setReadOnly(bReadOnly).
          setFile(fileDatabase).
          open();
      else
      {
        Database.FileFormat ff = Database.FileFormat.V2010; 
        int iExtension = sDatabaseName.lastIndexOf('.');
        if (iExtension >= 0)
        {
          if (sDatabaseName.substring(iExtension).toLowerCase().equals(".mdb"))
            ff = Database.FileFormat.V2003;
        }
        _db = new DatabaseBuilder().
          setReadOnly(false).
          setFileFormat(ff).
          setFile(fileDatabase).
          create();
      }
    }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage());  }
  } /* constructor AccessConnection */

  /*------------------------------------------------------------------*/
  /** {@link Connection} for JDK 1.7 */
  @Override
  public void abort(Executor executor) throws SQLException
  {
    throw new SQLException("aborting the connection is not supported!");
  } /* abort */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void close() throws SQLException
  {
    if (_db != null)
    {
      try 
      { 
        _db.flush(); 
        _db.close(); 
      }
      catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage());  }
      _db = null;
    }
  } /* close */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public boolean isClosed() throws SQLException
  {
    return (_db == null);
  } /* isClosed */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setReadOnly(boolean bReadOnly) throws SQLException
  {
    if (_bReadOnly != bReadOnly)
      throw new SQLException("Database writeability cannot be changed after it has been opened!");
  } /* setReadOnly */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public boolean isReadOnly() throws SQLException
  {
    return _bReadOnly;
  } /* isReadOnly */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public boolean isValid(int iTimeoutSeconds) throws SQLException
  {
    return !isClosed();
  } /* isValid */

  /*====================================================================
  Connection 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public boolean getAutoCommit() throws SQLException
  {
    return true;
  } /* getAutoCommit */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setAutoCommit(boolean bAutoCommit) throws SQLException
  {
    // Transactions are not supported
  } /* setAutoCommit */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Savepoint setSavepoint() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Transactions not supported for MS Access database!");
  } /* setSavepoint */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Savepoint setSavepoint(String sName) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Transactions not supported for MS Access database!");
  } /* setSavepoint */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void releaseSavepoint(Savepoint arg0) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Transactions not supported for MS Access database!");
  } /* releaseSavepoint */

  /*------------------------------------------------------------------*/
  /** {@link Connection} for JDK 1.7 */
  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Network timeouts not supported for MS Access database!");
  } /* setNetworkTimeout */

  /*------------------------------------------------------------------*/
  /** {@link Connection} for JDK 1.7 */
  @Override
  public int getNetworkTimeout() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Network timeouts not supported for MS Access database!");
  } /* getNetworkTimeout */
  
  /*------------------------------------------------------------------*/
  /** {@link Connection} for JDK 1.7 */
  @Override
  public void setSchema(String schema) throws SQLException
  {
    _schema = schema;
  } /* setSchema */

  /*------------------------------------------------------------------*/
  /** {@link Connection} for JDK 1.7 */
  @Override
  public String getSchema() throws SQLException
  {
    return _schema;
  } /* getSchema */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public int getTransactionIsolation() throws SQLException
  {
    return Connection.TRANSACTION_NONE;
  } /* getTransactionIsolation */
  
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setTransactionIsolation(int iLevel) throws SQLException
  {
    if (iLevel != Connection.TRANSACTION_NONE)
      throw new SQLException("Transactions not supported for MS Access database!");
  } /* setTransactionIsolation */
  
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void commit() throws SQLException
  {
    // Transactions are not supported
  } /* commit */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void rollback() throws SQLException
  {
    // Transactions are not supported
  } /* rollback */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException
  {
    // Transactions are not supported
  } /* rollback */

  /*====================================================================
  Statements 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public int getHoldability() throws SQLException
  {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  } /* getHoldability */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setHoldability(int iHoldability) throws SQLException
  {
    if (iHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
      throw new SQLFeatureNotSupportedException("Transactions not supported for MS Access database!");
  } /* setHoldability */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Statement createStatement() throws SQLException
  {
    return new AccessStatement(this);
  } /* createStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException
  {
    return createStatement();
  } /* createStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException
  {
    Statement stmt = null;
    if (resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT)
      stmt = createStatement();
    else
      throw new SQLFeatureNotSupportedException("ResultSet type holdability not supported for MS Access database!");
    return stmt;
  } /* createStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public CallableStatement prepareCall(String sSql) throws SQLException
  {
    throw new SQLException("Callable statements not supported for MS Access database!");
  } /* prepareCall */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public CallableStatement prepareCall(String sSql, int resultSetType, int resultSetConcurrency)
      throws SQLException
  {
    // CallableStatement cstmt = null;
      // cstmt = prepareCall(sSql);
    throw new SQLFeatureNotSupportedException("Callable statements not supported for MS Access database!");
    // return cstmt;
  } /* prepareCall */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public CallableStatement prepareCall(String sSql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
  {
    // CallableStatement cstmt = null;
    if (resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT)
      // cstmt = prepareCall(sSql);
      throw new SQLFeatureNotSupportedException("Callable statements not supported for MS Access database!");
    else
      throw new SQLFeatureNotSupportedException("ResultSet type or concurrency or holdability not supported for MS Access database!");
    // return cstmt;
  } /* prepareCall */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public PreparedStatement prepareStatement(String sSql)
      throws SQLException
  {
    return new AccessPreparedStatement(this,sSql);
  } /* prepareStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public PreparedStatement prepareStatement(String sSql, int iAutoGeneratedKeys)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Prepared statements with auto generated keys not supported for MS Access database!");
  } /* prepareStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public PreparedStatement prepareStatement(String sSql, int[] aiColumnIndexes)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Prepared statements with auto generated keys not supported for MS Access database!");
  } /* prepareStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public PreparedStatement prepareStatement(String sSql, String[] asColumnNames)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Prepared statements with auto generated keys not supported for MS Access database!");
  } /* prepareStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public PreparedStatement prepareStatement(String sSql, int iResultSetType, int iResultSetConcurrency) throws SQLException
  {
    PreparedStatement pstmt = null;
    if (iResultSetConcurrency == ResultSet.CONCUR_READ_ONLY)
      pstmt = prepareStatement(sSql);
    else
      throw new SQLFeatureNotSupportedException("ResultSet type or concurrency not supported for MS Access database!");
    return pstmt;
  } /* prepareStatement */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public PreparedStatement prepareStatement(String sSql, int iResultSetType, int iResultSetConcurrency, int iResultSetHoldability) throws SQLException
  {
    PreparedStatement pstmt = null;
    if ((iResultSetConcurrency == ResultSet.CONCUR_READ_ONLY) &&
        (iResultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT))
      pstmt = prepareStatement(sSql);
    else
      throw new SQLFeatureNotSupportedException("ResultSet type or concurrency or holdability not supported for MS Access database!");
    return pstmt;
  } /* prepareStatement */

  /*====================================================================
  large objects 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Blob createBlob() throws SQLException
  {
    return new AccessBlob();
  } /* createBlob */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Clob createClob() throws SQLException
  {
    return new AccessClob();
  } /* createClob */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public NClob createNClob() throws SQLException
  {
    return new AccessNClob();
  } /* createNClob */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public SQLXML createSQLXML() throws SQLException
  {
    return new AccessSqlXml();
  } /* createSQLXML */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Array createArrayOf(String sTypeName, Object[] ao)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Array creation not supported for MS Access database!");
  } /* createArrayOf */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Struct createStruct(String arg0, Object[] arg1)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("UDT creation not supported for MS Access database!");
  } /* createStruct */

  /*====================================================================
  SQL 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public String nativeSQL(String sql) throws SQLException
  {
    SqlFactory sf = new AccessSqlFactory();
    SqlStatement ss = sf.newSqlStatement();
    ss.parse(sql);
    sql = ss.format();
    return sql;
  } /* nativeSQL */

  /*====================================================================
  Meta data 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException
  {
    return new AccessDatabaseMetaData(this);
  } /* getMetaData */

  /*====================================================================
  UDTs 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("UDTs not supported for MS Access database!");
  }

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setTypeMap(Map<String, Class<?>> arg0)
      throws SQLException
  {
    throw new SQLFeatureNotSupportedException("UDTs not supported for MS Access database!");
  } /* setTypeMap */

  /*====================================================================
  Catalog 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public String getCatalog() throws SQLException
  {
    if (isClosed())
      throw new SQLException("getCatalog() called on closed connection!");
    // "the current catalog name or null if there is none"
    return null;
  }

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setCatalog(String arg0) throws SQLException
  {
    // "If the driver does not support catalogs, it will silently ignore this request."
  }

  /*====================================================================
  ClientInfo 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public Properties getClientInfo() throws SQLException
  {
    return new Properties();
  } /* getClientInfo */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public String getClientInfo(String sName) throws SQLException
  {
    return null;
  } /* getClientInfo */

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException
  {
    throw new SQLClientInfoException();
  }

  /*------------------------------------------------------------------*/
  /** {@link Connection} */
  @Override
  public void setClientInfo(String sName, String sValue)
      throws SQLClientInfoException
  {
    throw new SQLClientInfoException();
  }

  @Override
  public Blob createDatalinkObject() throws SQLException {
    return createBlob();
  }

} /* AccessConnection */
