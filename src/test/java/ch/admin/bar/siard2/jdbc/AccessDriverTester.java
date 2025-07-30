package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.*;
import static org.junit.Assert.*;
import org.junit.*;

public class AccessDriverTester
{
  // private static final File fileTEST_EMPTY_DATABASE = new File(_cp.getInstance()+"/"+_cp.getCatalog());
  private static final File fileTEST_EMPTY_DATABASE = new File("tmp/testempty.accdb");
  private static String _sDB_URL = AccessDriver.getUrl(fileTEST_EMPTY_DATABASE.getPath());
  private static final String _sDB_USER = "Admin";
  private static final String _sDB_PASSWORD = "";
  private static final String _sDRIVER_CLASS = "ch.admin.bar.siard2.jdbc.AccessDriver";
  private static final String _sINVALID_ACCESS_URL = "jdbc:oracle:thin:@//localhost:1521/orcl";;

  private Driver _driver = null;
  private Connection _conn = null;
  
  @Before
  public void setUp()
  {
    try { Class.forName(_sDRIVER_CLASS); }
    catch(ClassNotFoundException cnfe) { fail(cnfe.getClass().getName()+": "+cnfe.getMessage()); }
    try
    {
      _driver = DriverManager.getDriver(_sDB_URL);
      _conn = DriverManager.getConnection(_sDB_URL, _sDB_USER, _sDB_PASSWORD);
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* setUp */
  
  @After
  public void tearDown()
  {
    try
    {
      if ((_conn != null) && (!_conn.isClosed()))
        _conn.close();
      else
        fail("Connection cannot be closed!");
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* tearDown */

  @Test
  public void testWrapping()
  {
    assertSame("Registration of driver wrapper failed!", AccessDriver.class, _driver.getClass());
    assertSame("Choice of connection wrapper failed!", AccessConnection.class, _conn.getClass());
  } /* testWrapping */
  
  @Test
  public void testCompliant()
  {
    assertSame("Access driver not JDBC compliant!", true, _driver.jdbcCompliant());
  } /* testCompliant */
  
  @Test
  public void testAcceptsURL()
  {
    try
    {
      assertSame("Valid Access URL not accepted!", true, _driver.acceptsURL(_sDB_URL));
      assertSame("Invalid Access URL accepted!", false, _driver.acceptsURL(_sINVALID_ACCESS_URL));
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* testAcceptsURL */
  
  @Test
  public void testVersion()
  {
    int iMajorVersion = _driver.getMajorVersion();
    int iMinorVersion = _driver.getMinorVersion();
    String sVersion = String.valueOf(iMajorVersion)+"."+String.valueOf(iMinorVersion);
    assertEquals("Wrong Access Driver version "+sVersion+" found!", AccessDriver.sVERSION, sVersion);
  } /* testVersion */
  
  @Test
  public void testDriverProperties()
  {
    try
    {
      Properties props = new Properties();
      props.setProperty(AccessDriver.sPROP_USER, "TheUser");
      props.setProperty(AccessDriver.sPROP_PASSWORD, "ThePassword");
      props.setProperty(AccessDriver.sPROP_READ_ONLY,"false");
      DriverPropertyInfo[] apropInfo = _driver.getPropertyInfo(_sDB_URL, props);
      for (DriverPropertyInfo dpi: apropInfo)
        System.out.println(dpi.name+": "+dpi.value+" ("+String.valueOf(dpi.description)+")");
      assertSame("Unexpected driver properties!", 3, apropInfo.length);
    }
    catch(SQLException se) { fail(se.getClass().getName()+": "+se.getMessage()); }
  } /* testDriverProperties */

}
