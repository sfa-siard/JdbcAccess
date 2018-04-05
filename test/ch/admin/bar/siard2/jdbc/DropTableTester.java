package ch.admin.bar.siard2.jdbc;

import static org.junit.Assert.*;

import java.io.*;
import java.sql.*;
import org.junit.*;
import ch.admin.bar.siard2.jdbcx.*;
import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.sqlparser.identifier.*;

public class DropTableTester
{

  private static final ConnectionProperties _cp = new ConnectionProperties();
  private static final File fileTEST_EMPTY_DATABASE = new File(_cp.getInstance()+"/"+_cp.getCatalog());
  private static final File fileTEST_ACCESS_DATABASE = new File("tmp/testaccess.accdb");
  private static final String sUSER = _cp.getUser();
  private static final String sPASSWORD = _cp.getPassword();
  
  @Test
  public void test()
  {
    try 
    { 
      FU.copy(fileTEST_EMPTY_DATABASE, fileTEST_ACCESS_DATABASE);
      AccessDataSource dsAccess = new AccessDataSource();
      dsAccess.setDatabaseName(fileTEST_ACCESS_DATABASE.getAbsolutePath());
      dsAccess.setDescription("SQL data base");
      dsAccess.setReadOnly(false);
      dsAccess.setUser(sUSER);
      dsAccess.setPassword(sPASSWORD);
      AccessConnection connAccess = (AccessConnection)dsAccess.getConnection();
      connAccess.setAutoCommit(false);
      String sTableName = null;
      DatabaseMetaData dmd = connAccess.getMetaData();
      ResultSet rs = dmd.getTables(null, "%", "%", new String[] {"TABLE"});
      if (rs.next())
        sTableName = rs.getString("TABLE_NAME");
      rs.close();
      assertEquals("TABLETEST expected!","TABLETEST",sTableName);
      QualifiedId qiTable = new QualifiedId(null,"Admin",sTableName);
      Statement stmt = connAccess.createStatement();
      /**
      String sSql = "DELETE FROM "+qiTable.format();
      int iResult = stmt.executeUpdate(sSql);
      **/
      String sSql = "DROP TABLE "+qiTable.format()+" CASCADE"; // cascade implies delete!
      int iResult = stmt.executeUpdate(sSql);
      assertEquals("Drop failed!",0,iResult);
      stmt.close();
      rs = dmd.getTables(null, "%", "%", new String[] {"TABLE"});
      if (rs.next())
        fail("Table found: " + rs.getString("TABLE_NAME"));
      else
        System.out.println("No tables found!");
      rs.close();
      connAccess.close();
    }
    catch(SQLException se) { fail(EU.getExceptionMessage(se)); }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  }

}
