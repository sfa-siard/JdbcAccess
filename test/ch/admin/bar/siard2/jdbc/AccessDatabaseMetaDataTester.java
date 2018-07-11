package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;
import static org.junit.Assert.*;

import org.junit.*;

import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.utils.database.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.lang.*;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.access.*;
import ch.admin.bar.siard2.jdbcx.*;

public class AccessDatabaseMetaDataTester extends BaseDatabaseMetaDataTester
{
  private static final File fileTEST_EMPTY_DATABASE = new File("testfiles/testempty.accdb");
  private static final File fileTEST_ACCESS_SOURCE = new File("testfiles/testaccess.accdb");
  private static final File fileTEST_ACCESS_DATABASE = new File("tmp/testaccess.accdb");
  private static final File fileTEST_SQL_DATABASE = new File("tmp/testsql.accdb");
  private static final String sUSER = "Admin";
  private static final String sPASSWORD = "";
  private static Pattern _patTYPE = Pattern.compile("^(.*?)(\\(\\s*((\\d+)(\\s*,\\s*(\\d+))?)\\s*\\))?$");
  
  @BeforeClass
  public static void setUpClass()
  {
    try
    {
      FU.copy(fileTEST_EMPTY_DATABASE, fileTEST_ACCESS_DATABASE);
      /* The JDBC-ODBC bridge could still be used until JAVA 1.8 using
       * an extract from the JAVA 7 run-time library and the JdbcOdbc.dll.
       * Now that is blocked by the split packages prohibition.
       * So we use the test database originally created under JAVA 1.8.
       * If we ever want more controlled features in the test database 
       * we shall be in trouble ... (have to use JAVA 1.7 or 1.8!)
       */
      if (Execute.isOsWindows() && Execute.isJavaVersionLessThan("9"))
        new TestAccessDatabase(fileTEST_ACCESS_DATABASE);
      else
        FU.copy(fileTEST_ACCESS_SOURCE, fileTEST_ACCESS_DATABASE);
      FU.copy(fileTEST_EMPTY_DATABASE,fileTEST_SQL_DATABASE);
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
      setDatabaseMetaData(connAccess.getMetaData());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }
  
  @Before
  public void setUp()
  {
    setUp(true);
  } /* setUp */

  @Test
  public void testClass()
  {
    assertEquals("Wrong database meta data class!", AccessDatabaseMetaData.class, getDatabaseMetaData().getClass());
  } /* testClass */
  
  @Test
  public void testMatches()
  {
    try
    {
      BaseDatabaseMetaData bdmd = (BaseDatabaseMetaData)getDatabaseMetaData();
      assertTrue("Underscore in name fails!",AccessDatabaseMetaData.matches(bdmd.toPattern("ZLA_LAND"),"ZLA_LAND"));
      assertTrue("Percent in name fails!",AccessDatabaseMetaData.matches(bdmd.toPattern("ZLA%readme"),"ZLA%readme"));
      assertTrue("Other special character in name fails!",AccessDatabaseMetaData.matches(bdmd.toPattern("ZLA.LAND"),"ZLA.LAND"));
      assertTrue("Percent does not match all!",AccessDatabaseMetaData.matches("%","ZLA_readme"));
      assertTrue("Underscore does not match single letter!",AccessDatabaseMetaData.matches("ZLA_readme","ZLAPreadme"));
      assertTrue("Underscore does not match special character!",AccessDatabaseMetaData.matches("ZLA_readme","ZLA.readme"));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }
  
  @Override
  @Test
  public void testGetTypeInfo()
  {
    enter();
    try { print(getDatabaseMetaData().getTypeInfo()); }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Test
  public void testGetColumnsSqlSimple()
  {
    try 
    { 
      Map<String,TestColumnDefinition> mapCd = new HashMap<String,TestColumnDefinition>();
      for (int iColumn = 0; iColumn < TestSqlDatabase._listCdSimple.size(); iColumn++)
      {
        TestColumnDefinition tcd = TestSqlDatabase._listCdSimple.get(iColumn);
        mapCd.put(tcd.getName(), tcd);
      }
      QualifiedId qiSimple = TestSqlDatabase.getQualifiedSimpleTable();
      ResultSet rs = getDatabaseMetaData().getColumns(qiSimple.getCatalog(), qiSimple.getSchema(), qiSimple.getName(), "%");
      while (rs.next())
      {
        String sColumnName = rs.getString("COLUMN_NAME");
        int iDataType = rs.getInt("DATA_TYPE");
        String sTypeName = rs.getString("TYPE_NAME");
        int iColumnSize = rs.getInt("COLUMN_SIZE");
        switch(sTypeName)
        {
          case "BYTE": assertEquals("Invalid BYTE mapping!",Types.SMALLINT,iDataType); break;
          case "INT": assertEquals("Invalid INT mapping!",Types.SMALLINT,iDataType); break;
          case "LONG": assertEquals("Invalid LONG mapping!",Types.INTEGER,iDataType); break;
          case "MONEY": assertEquals("Invalid MONEY mapping!",Types.DECIMAL,iDataType); break;
          case "NUMERIC": assertEquals("Invalid NUMERIC mapping!",Types.NUMERIC,iDataType); break;
          case "FLOAT": assertEquals("Invalid FLOAT mapping!",Types.REAL,iDataType); break;
          case "DOUBLE": assertEquals("Invalid DOUBLE mapping!",Types.DOUBLE,iDataType); break;
          case "SHORT_DATE_TIME": assertEquals("Invalid SHORT_DATE_TIME mapping!",Types.TIMESTAMP,iDataType); break;
          case "TEXT": assertEquals("Invalid TEXT mapping!",Types.VARCHAR,iDataType); break;
          case "MEMO": assertEquals("Invalid MEMO mapping!",Types.CLOB,iDataType); break;
          case "BINARY": assertEquals("Invalid BINARY mapping!",Types.BINARY,iDataType); break;
          case "GUID": assertEquals("Invalid GUID mapping!",Types.BINARY,iDataType); break;
          case "OLE": assertEquals("Invalid OLE mapping!",Types.BLOB,iDataType); break;
          case "BOOLEAN": assertEquals("Invalid BIT mapping!",Types.BOOLEAN,iDataType); break;
          default: fail("Unexpected type name "+sTypeName+"!");
        }
        TestColumnDefinition tcd = mapCd.get(sColumnName);
        String sType = tcd.getType();
        // parse type
        if (!sType.startsWith("INTERVAL"))
        {
          Matcher matcher = _patTYPE.matcher(sType);
          if (matcher.matches())
          {
            /* compare column size with explicit precision */
            String sPrecision = matcher.group(4);
            if (sPrecision != null)
            {
              int iPrecision = Integer.parseInt(sPrecision);
              if (iDataType == Types.TIMESTAMP)
                iPrecision = iColumnSize;
              if ((iDataType == Types.DOUBLE) ||
                  (iDataType == Types.FLOAT) ||
                  (iDataType == Types.REAL))
                iPrecision = iColumnSize; // the explicit number of bits is irrelevant, the size is always 8 bytes
              assertEquals("Explicit precision does not match!",iPrecision,iColumnSize);
            }
          }
        }
      }
      rs.close();
      print(getDatabaseMetaData().getColumns(qiSimple.getCatalog(), qiSimple.getSchema(), qiSimple.getName(), "%"));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetColumnsSqlSimple */

  @Test
  public void testGetColumnsAccessSimple()
  {
    enter();
    tearDown();
    setUp(false);
    try 
    { 
      Map<String,TestColumnDefinition> mapCd = new HashMap<String,TestColumnDefinition>();
      for (int iColumn = 0; iColumn < TestAccessDatabase._listCdSimple.size(); iColumn++)
      {
        TestColumnDefinition tcd = TestAccessDatabase._listCdSimple.get(iColumn);
        mapCd.put(tcd.getName(), tcd);
      }
      QualifiedId qiSimple = TestAccessDatabase.getQualifiedSimpleTable();
      ResultSet rs = getDatabaseMetaData().getColumns(qiSimple.getCatalog(), qiSimple.getSchema(), qiSimple.getName(), "%");
      while (rs.next())
      {
        String sColumnName = rs.getString("COLUMN_NAME");
        int iDataType = rs.getInt("DATA_TYPE");
        String sTypeName = rs.getString("TYPE_NAME");
        int iColumnSize = rs.getInt("COLUMN_SIZE");
        switch(sTypeName)
        {
          case "BYTE": assertEquals("Invalid BYTE mapping!",Types.SMALLINT,iDataType); break;
          case "INT": assertEquals("Invalid INT mapping!",Types.SMALLINT,iDataType); break;
          case "LONG": assertEquals("Invalid LONG mapping!",Types.INTEGER,iDataType); break;
          case "MONEY": assertEquals("Invalid MONEY mapping!",Types.DECIMAL,iDataType); break;
          case "NUMERIC": assertEquals("Invalid NUMERIC mapping!",Types.NUMERIC,iDataType); break;
          case "FLOAT": assertEquals("Invalid FLOAT mapping!",Types.REAL,iDataType); break;
          case "DOUBLE": assertEquals("Invalid DOUBLE mapping!",Types.DOUBLE,iDataType); break;
          case "SHORT_DATE_TIME": assertEquals("Invalid SHORT_DATE_TIME mapping!",Types.TIMESTAMP,iDataType); break;
          case "TEXT": assertEquals("Invalid TEXT mapping!",Types.VARCHAR,iDataType); break;
          case "MEMO": assertEquals("Invalid MEMO mapping!",Types.CLOB,iDataType); break;
          case "BINARY": assertEquals("Invalid BINARY mapping!",Types.BINARY,iDataType); break;
          case "GUID": assertEquals("Invalid GUID mapping!",Types.CHAR,iDataType); break;
          case "OLE": assertEquals("Invalid OLE mapping!",Types.BLOB,iDataType); break;
          case "BOOLEAN": assertEquals("Invalid BIT mapping!",Types.BOOLEAN,iDataType); break;
          default: fail("Unexpected type name "+sTypeName+"!");
        }
        TestColumnDefinition tcd = mapCd.get(sColumnName);
        String sType = tcd.getType();
        if (sType.equals("GUID"))
          assertEquals("Invalid length for GUID string!",38,iColumnSize);
        else
        {
          // parse type
          Matcher matcher = _patTYPE.matcher(sType);
          if (matcher.matches())
          {
            /* compare column size with explicit precision */
            String sPrecision = matcher.group(4);
            if (sPrecision != null)
            {
              int iPrecision = Integer.parseInt(sPrecision);
              if (iDataType == Types.TIMESTAMP)
                iPrecision = iColumnSize;
              if ((iDataType == Types.DOUBLE) ||
                  (iDataType == Types.FLOAT) ||
                  (iDataType == Types.REAL))
                iPrecision = iColumnSize; // the explicit number of bits is irrelevant, the size is always 8 bytes
              assertEquals("Explicit precision does not match!",iPrecision,iColumnSize);
            }
          }
        }
      }
      rs.close();
      print(getDatabaseMetaData().getColumns(qiSimple.getCatalog(), qiSimple.getSchema(), qiSimple.getName(), "%"));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetColumnsAccessSimple */

  @Test
  public void testGetColumnsAccessComplex()
  {
    enter();
    tearDown();
    setUp(false);
    try 
    { 
      Map<String,TestColumnDefinition> mapCd = new HashMap<String,TestColumnDefinition>();
      for (int iColumn = 0; iColumn < TestAccessDatabase._listCdComplex.size(); iColumn++)
      {
        TestColumnDefinition tcd = TestAccessDatabase._listCdComplex.get(iColumn);
        mapCd.put(tcd.getName(), tcd);
      }
      QualifiedId qiComplex = TestAccessDatabase.getQualifiedComplexTable();
      ResultSet rs = getDatabaseMetaData().getColumns(qiComplex.getCatalog(), qiComplex.getSchema(), qiComplex.getName(), "%");
      while (rs.next())
      {
        String sColumnName = rs.getString("COLUMN_NAME");
        int iDataType = rs.getInt("DATA_TYPE");
        String sTypeName = rs.getString("TYPE_NAME");
        int iColumnSize = rs.getInt("COLUMN_SIZE");
        switch(sTypeName)
        {
          case "BYTE": assertEquals("Invalid BYTE mapping!",Types.SMALLINT,iDataType); break;
          case "INT": assertEquals("Invalid INT mapping!",Types.SMALLINT,iDataType); break;
          case "LONG": assertEquals("Invalid LONG mapping!",Types.INTEGER,iDataType); break;
          case "MONEY": assertEquals("Invalid MONEY mapping!",Types.DECIMAL,iDataType); break;
          case "NUMERIC": assertEquals("Invalid NUMERIC mapping!",Types.NUMERIC,iDataType); break;
          case "FLOAT": assertEquals("Invalid FLOAT mapping!",Types.REAL,iDataType); break;
          case "DOUBLE": assertEquals("Invalid DOUBLE mapping!",Types.DOUBLE,iDataType); break;
          case "SHORT_DATE_TIME": assertEquals("Invalid SHORT_DATE_TIME mapping!",Types.TIMESTAMP,iDataType); break;
          case "TEXT": assertEquals("Invalid TEXT mapping!",Types.VARCHAR,iDataType); break;
          case "MEMO": assertEquals("Invalid MEMO mapping!",Types.CLOB,iDataType); break;
          case "BINARY": assertEquals("Invalid BINARY mapping!",Types.BINARY,iDataType); break;
          case "GUID": assertEquals("Invalid GUID mapping!",Types.BINARY,iDataType); break;
          case "OLE": assertEquals("Invalid OLE mapping!",Types.BLOB,iDataType); break;
          case "BOOLEAN": assertEquals("Invalid BIT mapping!",Types.BOOLEAN,iDataType); break;
          case "VARCHAR(2) ARRAY[4]": assertEquals("Invalid multivalued ARRAY mapping!",Types.ARRAY,iDataType); break;
          case "BLOB ARRAY[127]": assertEquals("Invalid attachment ARRAY mapping!",Types.ARRAY,iDataType); break;
          default: fail("Unexpected type name "+sTypeName+"!");
        }
        TestColumnDefinition tcd = mapCd.get(sColumnName);
        String sType = tcd.getType();
        // parse type
        Matcher matcher = _patTYPE.matcher(sType);
        if (matcher.matches())
        {
          /* compare column size with explicit precision */
          String sPrecision = matcher.group(4);
          if (sPrecision != null)
          {
            if (iDataType == Types.TIMESTAMP)
              iColumnSize = iColumnSize - 20;
            int iPrecision = Integer.parseInt(sPrecision);
            assertEquals("Explicit precision does not match!",iPrecision,iColumnSize);
          }
        }
      }
      rs.close();
      print(getDatabaseMetaData().getColumns(qiComplex.getCatalog(), qiComplex.getSchema(), qiComplex.getName(), "%"));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetColumnsAccessComplex */
  
  @Test
  public void testGetColumnsViewSimple()
  {
    enter();
    tearDown();
    setUp(false);
    try
    {
      QualifiedId qiView = TestAccessDatabase.getQualifiedSimpleView();
      ResultSet rs = getDatabaseMetaData().getColumns(
        qiView.getCatalog(), 
        qiView.getSchema(), 
        qiView.getName(), "%");
      while(rs.next())
      {
        String sColumnName = rs.getString("COLUMN_NAME");
        int iDataType = rs.getInt("DATA_TYPE");
        String sTypeName = rs.getString("TYPE_NAME");
        System.out.println(sColumnName+": "+String.valueOf(iDataType)+" ("+SqlTypes.getTypeName(iDataType)+") "+sTypeName);
      }
      rs.close();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Test
  public void testGetColumnsViewComplex()
  {
    enter();
    tearDown();
    setUp(false);
    try
    {
      QualifiedId qiView = TestAccessDatabase.getQualifiedComplexView();
      ResultSet rs = getDatabaseMetaData().getColumns(
        qiView.getCatalog(), 
        qiView.getSchema(), 
        qiView.getName(), "%");
      while(rs.next())
      {
        String sColumnName = rs.getString("COLUMN_NAME");
        int iDataType = rs.getInt("DATA_TYPE");
        String sTypeName = rs.getString("TYPE_NAME");
        System.out.println(sColumnName+": "+String.valueOf(iDataType)+" ("+SqlTypes.getTypeName(iDataType)+") "+sTypeName);
      }
      rs.close();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }

  @Test
  public void testGetIndexInfo()
  {
    enter();
    try 
    { 
      ResultSet rs = getDatabaseMetaData().getIndexInfo(null,null,"TABLETEST",true,false);
      while (rs.next())
      {
        boolean bNonUnique = rs.getBoolean("NON_UNIQUE");
        if (!bNonUnique)
        {
          String sIndexName = rs.getString("INDEX_NAME");
          int iIndexType = rs.getInt("TYPE");
          String sIndexType = null;
          switch(iIndexType)
          {
            case DatabaseMetaData.tableIndexClustered: sIndexType = "tableIndexClustered"; break;
            case DatabaseMetaData.tableIndexHashed: sIndexType = "tableIndexHashed"; break;
            case DatabaseMetaData.tableIndexOther: sIndexType = "tableIndexOther"; break;
            case DatabaseMetaData.tableIndexStatistic: sIndexType = "tableIndexStatistic"; break;
          }
          int iOrdinalPosition = rs.getInt("ORDINAL_POSITION");
          String sColumnName = rs.getString("COLUMN_NAME");
          System.out.println(sIndexName+": "+sIndexType+", "+String.valueOf(iOrdinalPosition)+" "+sColumnName);
        }
        else
          System.err.println("Unexpected non-unique index found!!!");
      }
      rs.close();
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }
  
  /***
  @Test
  public void testGetImportedKeys()
  {
    enter();
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
      AccessDatabaseMetaData dmdAccess = (AccessDatabaseMetaData)connAccess.getMetaData();
      ResultSet rs = dmdAccess.getImportedKeys(null,"Admin","Main");
      rs.close();
    } 
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  } /* testGetImportedKeys */
  
} /* AccessDatabaseMetaDataTester */
