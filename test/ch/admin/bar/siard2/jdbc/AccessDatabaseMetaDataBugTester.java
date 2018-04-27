package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;
import static org.junit.Assert.*;

import org.junit.*;

import ch.enterag.utils.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.database.*;
import ch.enterag.sqlparser.identifier.*;
import ch.admin.bar.siard2.jdbcx.*;

public class AccessDatabaseMetaDataBugTester extends BaseDatabaseMetaDataTester
{
  private static final File fileTEST_ACCESS_SOURCE = new File("../Bugs/20180426/Datenbank_UEB.accdb");
  private static final File fileTEST_ACCESS_DATABASE = new File("tmp/Datenbank_UEB.accdb");
  private static final String sUSER = "Admin";
  private static final String sPASSWORD = "";
  
  @BeforeClass
  public static void setUpClass()
  {
    try
    {
      FU.copy(fileTEST_ACCESS_SOURCE, fileTEST_ACCESS_DATABASE);
    }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  } /* setUpClass */

  @Before
  public void setUp()
  {
    try 
    { 
      AccessDataSource dsAccess = new AccessDataSource();
      dsAccess.setDatabaseName(fileTEST_ACCESS_DATABASE.getAbsolutePath());
      dsAccess.setDescription("Access data base");
      dsAccess.setReadOnly(false);
      dsAccess.setUser(sUSER);
      dsAccess.setPassword(sPASSWORD);
      AccessConnection connAccess = (AccessConnection)dsAccess.getConnection();
      connAccess.setAutoCommit(false);
      setDatabaseMetaData(connAccess.getMetaData());
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  }
  
  @Test
  public void testGetColumnsView()
  {
    try
    {
      QualifiedId qiView = new QualifiedId(null,"Admin","NachnamenMitarbeiter");
      ResultSet rs = getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%");
      while (rs.next())
      {
        String sColumnName = rs.getString("COLUMN_NAME");
        int iDataType = rs.getInt("DATA_TYPE");
        String sTypeName = rs.getString("TYPE_NAME");
        System.out.println(sColumnName+": "+sTypeName+" ("+SqlTypes.getTypeName(iDataType)+")");
      }
      rs.close();
      print(getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%"));
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetColumnsView */

} /* AccessDatabaseMetaDataTester */
