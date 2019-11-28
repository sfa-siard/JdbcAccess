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
  private static final int iBug = Integer.parseInt(System.getProperty("bug"));
  private static final File fileTEST_ACCESS_SOURCE;
  private static final File fileTEST_ACCESS_DATABASE;
  static
  {
    if (iBug == 460)
    {
      fileTEST_ACCESS_SOURCE = new File("../Bugs/460/Art1.accdb");
      fileTEST_ACCESS_DATABASE = new File("tmp/Art1.accdb");
    }
    else if (iBug == 461)
    {
      fileTEST_ACCESS_SOURCE = new File("../Bugs/461/coffee.mdb");
      fileTEST_ACCESS_DATABASE = new File("tmp/coffee.mdb");
    }
    else if (iBug == 20180426) 
    {
      fileTEST_ACCESS_SOURCE = new File("../Bugs/20180426/Datenbank_UEB.accdb");
      fileTEST_ACCESS_DATABASE = new File("tmp/Datenbank_UEB.accdb");
    }
    else if (iBug == 20190123) 
    {
      fileTEST_ACCESS_SOURCE = new File("../Bugs/20190123/simpleDB/simpleDB.accdb");
      fileTEST_ACCESS_DATABASE = new File("tmp/simpleDB.accdb");
    }
    else if (iBug == 10) 
    {
      fileTEST_ACCESS_SOURCE = new File("../Bugs/Issue10/Database_frontend/Database_frontend.accdb");
      fileTEST_ACCESS_DATABASE = new File("tmp/Database_frontend.accdb");
    }
    else if (iBug == 13)
    {
      fileTEST_ACCESS_SOURCE = new File("testfiles/Northwind.accdb");
      fileTEST_ACCESS_DATABASE = new File("tmp/Northwind.accdb");
    }
    else 
    {
      fileTEST_ACCESS_SOURCE = new File("testfiles/testaccess.accdb");
      fileTEST_ACCESS_DATABASE = new File("tmp/testaccess.accdb");
    }
  }
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
  public void testGetColumns()
  {
    try
    {
      if (iBug == 461)
      {
        QualifiedId qiView = new QualifiedId(null,"Admin","CoffeeChain Query");
        int iColumn = 0;
        ResultSet rs = getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%");
        while (rs.next())
        {
          String sColumnName = rs.getString("COLUMN_NAME");
          int iDataType = rs.getInt("DATA_TYPE");
          String sTypeName = rs.getString("TYPE_NAME");
          iColumn++;
          int iPosition = rs.getInt("ORDINAL_POSITION");
          assertEquals("Wrong position!",iColumn,iPosition);
          System.out.println(sColumnName+": "+sTypeName+" ("+SqlTypes.getTypeName(iDataType)+")");
        }
        rs.close();
        print(getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%"));
      }
      else if (iBug == 460)
      {
        QualifiedId qiView = new QualifiedId(null,"Admin","Artwork City Query");
        int iColumn = 0;
        ResultSet rs = getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%");
        while (rs.next())
        {
          String sColumnName = rs.getString("COLUMN_NAME");
          int iDataType = rs.getInt("DATA_TYPE");
          String sTypeName = rs.getString("TYPE_NAME");
          iColumn++;
          int iPosition = rs.getInt("ORDINAL_POSITION");
          assertEquals("Wrong position!",iColumn,iPosition);
          System.out.println(sColumnName+": "+sTypeName+" ("+SqlTypes.getTypeName(iDataType)+")");
        }
        rs.close();
        print(getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%"));
      }
      else if (iBug == 20180426)
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
      else if (iBug == 20190123)
      {
        QualifiedId qiView = new QualifiedId(null,"Admin","qry\\_Personen");
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
      else if (iBug == 10)
      {
        QualifiedId qiView = new QualifiedId(null,"Admin","BerichtLang");
        int iColumn = 0;
        ResultSet rs = getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%");
        while (rs.next())
        {
          String sColumnName = rs.getString("COLUMN_NAME");
          int iDataType = rs.getInt("DATA_TYPE");
          String sTypeName = rs.getString("TYPE_NAME");
          iColumn++;
          int iPosition = rs.getInt("ORDINAL_POSITION");
          assertEquals("Wrong position!",iColumn,iPosition);
          System.out.println(sColumnName+": "+sTypeName+" ("+SqlTypes.getTypeName(iDataType)+")");
        }
        rs.close();
        print(getDatabaseMetaData().getColumns(qiView.getCatalog(), qiView.getSchema(), qiView.getName(), "%"));
      }
      else if (iBug == 13)
      {
        //QualifiedId qiTable = new QualifiedId(null,"Admin","Inventory on Order");
        //QualifiedId qiTable = new QualifiedId(null,"Admin","Suppliers Extended");
        //QualifiedId qiTable = new QualifiedId(null,"Admin","Invoice Data");
        //QualifiedId qiTable = new QualifiedId(null,"Admin","Inventory");
        QualifiedId qiTable = new QualifiedId(null,"Admin","Sales Analysis");
        print(getDatabaseMetaData().getColumns(qiTable.getCatalog(), qiTable.getSchema(), qiTable.getName(), "%"));
      }
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
  } /* testGetColumnsView */
  
} /* AccessDatabaseMetaDataTester */
