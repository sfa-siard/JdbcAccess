package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.*;
import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.lang.Execute;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.access.*;
import ch.admin.bar.siard2.jdbcx.*;

public class AccessResultSetMetaDataTester extends BaseResultSetMetaDataTester
{
  private static String getTableQuery(QualifiedId qiTable, List<TestColumnDefinition> listCd)
  {
    StringBuilder sbSql = new StringBuilder("SELECT\r\n  ");
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      if (iColumn > 0)
        sbSql.append(",\r\n  ");
      TestColumnDefinition tcd = listCd.get(iColumn);
      sbSql.append(SqlLiterals.formatId(tcd.getName()));
    }
    sbSql.append("\r\nFROM ");
    sbSql.append(qiTable.format());
    return sbSql.toString();
  } /* getTableQuery */
  
  private static String _sNativeQuerySimple = getTableQuery(TestAccessDatabase.getQualifiedSimpleTable(),TestAccessDatabase._listCdSimple);
  private static String _sNativeQueryComplex = getTableQuery(TestAccessDatabase.getQualifiedComplexTable(),TestAccessDatabase._listCdComplex);
  private static String _sSqlQuerySimple = getTableQuery(TestSqlDatabase.getQualifiedSimpleTable(),TestSqlDatabase._listCdSimple);
  
  private static final File fileTEST_EMPTY_DATABASE = new File("testfiles/testempty.accdb");
  private static final File fileTEST_ACCESS_SOURCE = new File("testfiles/testaccess.accdb");
  private static final File fileTEST_ACCESS_DATABASE = new File("tmp/testaccess.accdb");
  private static final File fileTEST_SQL_DATABASE = new File("tmp/testsql.accdb");
  private static final String sUSER = "Admin";
  private static final String sPASSWORD = "";
  
  @BeforeClass
  public static void setUpClass()
  {
    try
    {
      FU.copy(fileTEST_EMPTY_DATABASE, fileTEST_ACCESS_DATABASE);
      if (Execute.isOsWindows())
        new TestAccessDatabase(fileTEST_ACCESS_DATABASE);
      else
        FU.copy(fileTEST_ACCESS_SOURCE, fileTEST_ACCESS_DATABASE);
      FU.copy(fileTEST_EMPTY_DATABASE, fileTEST_SQL_DATABASE);
      AccessDataSource dsAccess = new AccessDataSource();
      dsAccess.setDatabaseName(fileTEST_SQL_DATABASE.getAbsolutePath());
      dsAccess.setDescription("SQL data base");
      dsAccess.setReadOnly(false);
      dsAccess.setUser(sUSER);
      dsAccess.setPassword(sPASSWORD);
      AccessConnection connAccess = (AccessConnection)dsAccess.getConnection();
      new TestSqlDatabase(connAccess);
      connAccess.close();
    }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* setUpClass */

  private void openResultSet(boolean bSql, String sQuery)
    throws SQLException
  {
    tearDown();
    AccessDataSource dsAccess = new AccessDataSource();
    if (bSql)
    {
      dsAccess.setDatabaseName(fileTEST_SQL_DATABASE.getAbsolutePath());
      dsAccess.setDescription("SQL data base");
    }
    else
    {
      dsAccess.setDatabaseName(fileTEST_ACCESS_DATABASE.getAbsolutePath());
      dsAccess.setDescription("Access data base");
    }
    dsAccess.setReadOnly(false);
    dsAccess.setUser(sUSER);
    dsAccess.setPassword(sPASSWORD);
    AccessConnection connAccess = (AccessConnection)dsAccess.getConnection();
    connAccess.setAutoCommit(false);
    Statement stmt = connAccess.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = stmt.executeQuery(sQuery);
    ResultSetMetaData rsmd = rs.getMetaData();
    setResultSetMetaData(rsmd,rs);
  } /* openResultSet */
  
  @Before
  public void setUp()
  {
    try { openResultSet(true,_sSqlQuerySimple); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* setUp */

  @Test
  public void testClass()
  {
    assertEquals("Wrong database meta data class!", AccessResultSetMetaData.class, getResultSetMetaData().getClass());
  } /* testClass */

  @Test
  public void testNativeSimple()
  {
    try
    {
      openResultSet(false,_sNativeQuerySimple);
      super.testAll();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testNativeSimple */

  @Test
  public void testNativeComplex()
  {
    try
    {
      openResultSet(false,_sNativeQueryComplex);
      super.testAll();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testNativeComplex */

  @Test
  public void testSqlSimple()
  {
    try
    {
      openResultSet(true,_sSqlQuerySimple);
      super.testAll();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testSqlSimple */

}
