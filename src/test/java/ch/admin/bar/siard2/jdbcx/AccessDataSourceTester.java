package ch.admin.bar.siard2.jdbcx;

import ch.admin.bar.siard2.jdbc.AccessConnection;
import ch.enterag.utils.FU;
import org.junit.*;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class AccessDataSourceTester {
    private static final File fileTEST_DATABASE = new File("src/test/resources/testfiles/TestDataSource.accdb");
    private static final File fileBACKUP_DATABASE = new File("src/test/resources/tmp/TestDataSource.bak");
    private static final String sDESCRIPTION = "Test of AccessDataSource";
    private static final boolean bREAD_ONLY = true;
    private static final String sUSER = "Admin";
    private AccessDataSource _ds = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        boolean bCopied = false;
        try {
            if (fileBACKUP_DATABASE.exists())
                FU.copy(fileBACKUP_DATABASE, fileTEST_DATABASE);
            else
                FU.copy(fileTEST_DATABASE, fileBACKUP_DATABASE);
            bCopied = true;
        } catch (Exception e) {
        }
        if (!bCopied)
            fail("Initial copying failed!");
        _ds = new AccessDataSource();
        _ds.setDatabaseName(fileTEST_DATABASE.getAbsolutePath());
        _ds.setDescription(sDESCRIPTION);
        _ds.setReadOnly(bREAD_ONLY);
        _ds.setUser(sUSER);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetDatabaseName() {
        System.out.println("\nGetDatabaseName");
        assertEquals("Invalid database name!", fileTEST_DATABASE.getAbsolutePath(), _ds.getDatabaseName());
    }

    @Test
    public void testGetUrl() {
        System.out.println("\nGetUrl");
        assertEquals("Invalid URL!", "jdbc:access:" + fileTEST_DATABASE.getAbsolutePath(), _ds.getUrl());
    }

    @Test
    public void testGetDescription() {
        System.out.println("\nGetDescription");
        assertEquals("Wrong description!", sDESCRIPTION, _ds.getDescription());
    }

    @Test
    public void testGetUser() {
        System.out.println("\nGetUser");
        assertEquals("Invalid user!", sUSER, _ds.getUser());
        System.out.println("User: " + _ds.getUser());
    }

    @Test
    public void testGetReadOnly() {
        System.out.println("\nGetReadOnly");
        assertEquals("Invalid read-only value!", bREAD_ONLY, _ds.getReadOnly());
    }

    @Test
    public void testIsWrapperFor() {
        System.out.println("\nIsWrapperFor");
        try {
            assertTrue("IsWrapperFor failed!", _ds.isWrapperFor(DataSource.class));
            assertFalse("IsWrapperFor did not fail!", _ds.isWrapperFor(Connection.class));
        } catch (SQLException se) {
            fail(se.getClass()
                   .getName() + ": " + se.getMessage());
        }
    }

    @Test
    public void testUnwrap() {
        System.out.println("\nUnwrap");
        try {
            DataSource ds = _ds.unwrap(DataSource.class);
            assertNotNull("Unwrap failed!", ds);
        } catch (SQLException se) {
            fail(se.getClass()
                   .getName() + ": " + se.getMessage());
        }
        try {
            Connection conn = _ds.unwrap(Connection.class);
            assertNull("Unwrap did not fail!", conn);
        } catch (SQLException se) {
        }
    }

    @Test
    public void testGetLoginTimeout() {
        System.out.println("\nGetLoginTimeout");
        try {
            assertEquals("Invalid login timeout!", 0, _ds.getLoginTimeout());
        } catch (SQLException se) {
            fail(se.getClass()
                   .getName() + ": " + se.getMessage());
        }
    }

    @Test
    public void testGetConnection() {
        System.out.println("\nGetConnection");
        try {
            Connection conn = _ds.getConnection();
            assertEquals("Invalid class!", AccessConnection.class, conn.getClass());
            conn.close();
        } catch (SQLException se) {
            fail(se.getClass()
                   .getName() + ": " + se.getMessage());
        }
    }

    @Test
    public void testGetConnectionStringString() {
        System.out.println("\nGetConnection(\"Admin\",\"\")");
        try {
            Connection conn = _ds.getConnection(sUSER, "");
            assertEquals("Invalid class!", AccessConnection.class, conn.getClass());
            conn.close();
        } catch (SQLException se) {
            fail(se.getClass()
                   .getName() + ": " + se.getMessage());
        }
    }

}
