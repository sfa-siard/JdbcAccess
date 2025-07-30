/*======================================================================
AccessDataSource implements a wrapped Jackcess DataSource for MS Access.
Application : SIARD2
Description : AccessDataSource implements a wrapped Jackcess DataSource 
              for MS Access.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 04.11.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.jdbcx;

import java.io.*;
import java.sql.*;
import javax.sql.*;
import java.util.logging.*;
import ch.admin.bar.siard2.jdbc.*;

/*====================================================================*/
/** AccessDataSource implements a wrapped Jackcess DataSource for 
 * MS Access.
 * @author Hartwig Thomas
 */
public class AccessDataSource
  implements DataSource
{
  /* DataSource properties */
  /** File name of an MS Access database */
  private String _sDatabaseName = null;
  /** @return file name of MS Access database. */
  public String getDatabaseName() { return _sDatabaseName; }
  /** @param sDatabaseName file name of MS Access database. */
  public void setDatabaseName(String sDatabaseName)
  {
    File fileDatabase = new File(sDatabaseName);
    _sDatabaseName = fileDatabase.getAbsolutePath();
  }
  /** @return JDBC URL associated with this database */
  public String getUrl() { return AccessDriver.getUrl(getDatabaseName()); }
  public void setUrl(String sUrl) { setDatabaseName(AccessDriver.getDatabaseName(sUrl)); }
  /** Description of this data source */
  private String _sDescription = null;
  /** @return description of data source. */
  public String getDescription() { return _sDescription; }
  /** @param sDescription description of data source. */
  public void setDescription(String sDescription)
  { _sDescription = sDescription; }
  /** Password of database user */
  private String _sPassword = null;
  /** @return password for database session. */ 
  public String getPassword() { return _sPassword; }
  /** @param sPassword password for database session. */
  public void setPassword(String sPassword) { _sPassword = sPassword; }
  /** Database user */
  private String _sUser = AccessDriver.sDEFAULT_USER;
  /** @return user for database session */
  public String getUser() { return _sUser; }
  /** @param sUser user for database session */
  public void setUser(String sUser) { _sUser = sUser; }
  /** Read-only open mode */
  private boolean _bReadOnly = false;
  /** @return true, if database is to be opened read-only. */
  public boolean getReadOnly() { return _bReadOnly; }
  /** @param bReadOnly true, if database is to be opened read-only. */
  public void setReadOnly(boolean bReadOnly) { _bReadOnly = bReadOnly; }

  public AccessDataSource()
  {
  } /* constructor */
  
  public AccessDataSource(String sUrl, String sUser, String sPassword)
    throws SQLException
  {
    setUrl(sUrl);
    setUser(sUser);
    setPassword(sPassword);
  } /* constructor AccessDataSource */
  
  /*====================================================================
  Wrapper 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link ResultSetMetaData} */
  @Override
  public boolean isWrapperFor(Class<?> clsInterface) throws SQLException
  {
    return clsInterface.equals(DataSource.class);
  } /* isWrapperFor */
  /*------------------------------------------------------------------*/
  /** {@link ResultSetMetaData} */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clsInterface) throws SQLException
  {
    T impl = null;
    if (isWrapperFor(clsInterface))
      impl = (T)this;
    else
      throw new SQLException("AccessDataSource cannot be unwrapped to "+clsInterface.getName()+"!");
    return impl;
  } /* unwrap */

  /*====================================================================
  Interface properties 
  ====================================================================*/
  /** LoginTimeout is really ignored */
  private int _iLoginTimeoutSeconds = 0;
  /** {@link DataSource} */
  @Override
  public int getLoginTimeout() throws SQLException { return _iLoginTimeoutSeconds; }
  /** {@link DataSource} */
  @Override
  public void setLoginTimeout(int iLoginTimeoutSeconds) throws SQLException { _iLoginTimeoutSeconds = iLoginTimeoutSeconds; }
  /** PrintWriter for database logging */
  private PrintWriter _pwLogWriter = new PrintWriter(System.out);
  /** {@link DataSource} */
  @Override
  public PrintWriter getLogWriter() throws SQLException { return _pwLogWriter; }
  /** {@link DataSource} */
  @Override
  public void setLogWriter(PrintWriter pwLogWriter) throws SQLException { _pwLogWriter = pwLogWriter; }
  
  /*======================================================================
  Interface methods 
  ======================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link DataSource} for JDK 1.7
  @Override
  public Logger getParentLogger()
      throws SQLFeatureNotSupportedException
  {
    throw new SQLFeatureNotSupportedException("ParentLogger not supported!");
  } /* getParentLogger */

  /*------------------------------------------------------------------*/
  /** {@link DataSource} */
  @Override
  public Connection getConnection() throws SQLException
  {
    return new AccessConnection(_sDatabaseName, _sUser, _sPassword, _bReadOnly, _pwLogWriter);
  } /* getConnection */

  /*------------------------------------------------------------------*/
  /** {@link DataSource} */
  @Override
  public Connection getConnection(String sUser, String sPassword)
      throws SQLException
  {
    setUser(sUser);
    setPassword(sPassword);
    return getConnection();
  } /* getConnection */
  
  /*------------------------------------------------------------------*/
  /** {@link Driver} for JDK 1.7 */
  @Override
  public Logger getParentLogger()
      throws SQLFeatureNotSupportedException
  {
    throw new SQLFeatureNotSupportedException("ParentLogger not supported!");
  } /* getParentLogger */

} /* AccessDataSource */
