package ch.admin.bar.siard2.jdbc;


import java.io.*;
import java.sql.*;

import static org.junit.Assert.*;
import org.junit.*;

import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.database.SqlTypes;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.lang.Execute;
import ch.enterag.sqlparser.SqlLiterals;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.access.*;
import ch.admin.bar.siard2.jdbcx.*;

public class AccessStatementTester extends BaseStatementTester
{
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

  private void setUp(boolean bSql)
  {
    try 
    { 
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
      setStatement(connAccess.createStatement());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }
  
  @Before
  public void setUp()
  {
    setUp(false);
  } /* setUp */

  
  @Test
  public void testClass()
  {
    assertEquals("Wrong statement class!", AccessStatement.class, getStatement().getClass());
  } /* testClass */

  @Override
  @Test
  public void testGetMoreResults()
  {
    enter();
    try { getStatement().getMoreResults(); }
    catch(SQLException se) { System.out.println(EU.getExceptionMessage(se)); }
  } /* testGetMoreResults */

  @Test
  public void testExecuteUpdate()
  {
    try
    {
      String sSql = "CREATE SCHEMA TESTSCHEMA"; 
      Statement stmt = getStatement();
      int iResult = stmt.executeUpdate(sSql);
      assertEquals("Invalid result!",0,iResult);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    /***/
    tearDown();
    setUpClass();
    /***/
  } /* testExcuteUpdate */
  
  @Test
  public void testCreateTable()
  {
    try
    {
      String sSql = "CREATE TABLE \"Admin\".TABLETEST1(\r\n"+
        "  \"id\" INTEGER NOT NULL,\r\n" +
        "  COLNUMERIC NUMERIC(18),\r\n" +
        "  COLDECIMAL DECIMAL,\r\n" +
        "  COLCHAR CHAR,\r\n" +
        "  COLTEXT VARCHAR(256),\r\n" +
        "  COLMEMO CLOB,\r\n" +
        "  COLLONG INTEGER,\r\n" +
        "  COLINT SMALLINT,\r\n" +
        "  COLBYTE SMALLINT,\r\n" +
        "  COLDOUBLE DOUBLE PRECISION,\r\n" +
        "  COLFLOAT REAL,\r\n" +
        "  COLDATETIME TIMESTAMP,\r\n" +
        "  COLDATE TIMESTAMP,\r\n" +
        "  COLTIME TIMESTAMP,\r\n" +
        "  COLMONEY DECIMAL(19, 4),\r\n" +
        "  COLBOOLEAN BOOLEAN,\r\n" +
        "  \"COLLOOKUP.COLLOOKUP[1]\" VARCHAR(2),\r\n" +
        "  \"COLLOOKUP.COLLOOKUP[2]\" VARCHAR(2),\r\n" +
        "  COLRICHTEXT CLOB,\r\n" +
        "  \"COLATTACH.COLATTACH[1]\" BLOB,\r\n" +
        "  COLOLE BLOB,\r\n" +
        "  COLLINK CLOB,\r\n" +
        "  PRIMARY KEY(\"id\"))";      
      Statement stmt = getStatement();
      int iResult = stmt.executeUpdate(sSql);
      assertEquals("Invalid result!",0,iResult);
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    /***/
    tearDown();
    setUpClass();
    /***/
  } /* testExcuteUpdate */
  
  @Test
  public void testExecuteQuery()
  {
    try
    {
      QualifiedId qiTable = TestAccessDatabase.getQualifiedComplexTable();
      String sQuery = "SELECT COUNT(*) AS RECORDS, SUM(OCTET_LENGTH(\"COLMEMO\")) AS \"COLMEMO_SIZE\" FROM "+qiTable.format();
      Statement stmt = getStatement();
      ResultSet rs = stmt.executeQuery(sQuery);
      if (rs != null)
      {
        while (rs.next())
        {
          int iRecords = rs.getInt("RECORDS");
          int iSize = rs.getInt("COLMEMO_SIZE");
          System.out.println("Records: "+String.valueOf(iRecords)+", Size: "+String.valueOf(iSize));
        }
        rs.close();
      }
      stmt.close();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testExecuteQuery */
  
  @Test
  public void testExecuteSelectSizes()
  {
    try
    {
      QualifiedId qiTable = TestAccessDatabase.getQualifiedComplexTable();
      BaseDatabaseMetaData bdmd = (BaseDatabaseMetaData)getStatement().getConnection().getMetaData();
      StringBuilder sbSql = new StringBuilder("SELECT COUNT(*) AS RECORDS");
      for (int iColumn = 0; iColumn < TestAccessDatabase._listCdComplex.size(); iColumn++)
      {
        TestColumnDefinition tcd = TestAccessDatabase._listCdComplex.get(iColumn);
        String sColumnName = tcd.getName();
        int iDataType = Types.NULL;
        String sTypeName = null;
        ResultSet rsColumns = bdmd.getColumns(
          qiTable.getCatalog(), 
          bdmd.toPattern(qiTable.getSchema()),
          bdmd.toPattern(qiTable.getName()),
          bdmd.toPattern(sColumnName));
        if (rsColumns.next())
        {
          iDataType = rsColumns.getInt("DATA_TYPE");
          sTypeName = rsColumns.getString("TYPE_NAME");
        }
        rsColumns.close();
        System.out.println(sColumnName+": "+String.valueOf(iDataType)+" ("+SqlTypes.getTypeName(iDataType)+") "+sTypeName);
        if ((iDataType == Types.BLOB) ||
            (iDataType == Types.CLOB) ||
            (iDataType == Types.NCLOB))
        {
          sbSql.append(",\r\n  SUM(OCTET_LENGTH(");
          sbSql.append(SqlLiterals.formatId(sColumnName));
          sbSql.append(")) AS ");
          sbSql.append(SqlLiterals.formatId(sColumnName+"_SIZE"));
        }
      }
      sbSql.append("\r\nFROM ");
      sbSql.append(qiTable.format());
      ResultSet rs = getStatement().executeQuery(sbSql.toString());
      ResultSetMetaData rsmd = rs.getMetaData();
      while(rs.next())
      {
        for (int iColumn = 0; iColumn < rsmd.getColumnCount(); iColumn++)
        {
          String sColumnName = rsmd.getColumnLabel(iColumn+1);
          long lValue = rs.getLong(iColumn+1);
          System.out.println(sColumnName+": "+String.valueOf(lValue));
        }
      }
      rs.close();
    }
    catch(SQLException se) { System.out.println(EU.getExceptionMessage(se)); }
  } /* testExecuteSelectSize */
  
  /***
  @Test
  public void testEmptySelectSize()
  {
    File fileBugDatabase = new File("..\\Bugs\\445\\Empty.accdb");
    File fileAccessDatabase = new File("logs\\Empty.accdb");
    try
    {
      FU.copy(fileBugDatabase, fileAccessDatabase);
      AccessDataSource dsAccess = new AccessDataSource();
      dsAccess.setDatabaseName(fileAccessDatabase.getAbsolutePath());
      dsAccess.setDescription("Bug 445 data base");
      dsAccess.setReadOnly(false);
      dsAccess.setUser(sUSER);
      dsAccess.setPassword(sPASSWORD);
      AccessConnection connAccess = (AccessConnection)dsAccess.getConnection();
      connAccess.setAutoCommit(false);
      AccessStatement stmtAccess = (AccessStatement)connAccess.createStatement();
      ResultSet rs = stmtAccess.executeQuery("SELECT COUNT(*) AS RECORDS FROM \"Admin\".\"AFGDistrict\"");
      if (rs.next())
      {
        long l = rs.getLong("RECORDS");
        System.out.println("Records: "+String.valueOf(l));
        assertEquals("Invalid record count for empty table!",0l,l);
      }
      else
        fail("No number of records determined!");
      rs.close();
      stmtAccess.close();
      connAccess.close();
    }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }
  ***/
} /* AccessStatementTester */
