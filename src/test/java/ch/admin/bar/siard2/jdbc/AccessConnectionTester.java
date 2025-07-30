package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.sql.*;

import static org.junit.Assert.*;

import org.junit.*;

import ch.enterag.utils.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.utils.lang.*;
import ch.admin.bar.siard2.access.*;
import ch.admin.bar.siard2.jdbcx.*;

public class AccessConnectionTester extends BaseConnectionTester {
    private static final File fileTEST_EMPTY_DATABASE = new File("testfiles/testempty.accdb");
    private static final File fileTEST_ACCESS_SOURCE = new File("testfiles/testaccess.accdb");
    private static final File fileTEST_ACCESS_DATABASE = new File("tmp/testaccess.accdb");
    private static final File fileTEST_SQL_DATABASE = new File("tmp/testsql.accdb");
    private static final String sUSER = "Admin";
    private static final String sPASSWORD = "";

    @BeforeClass
    public static void setUpClass() throws IOException, SQLException {
        FU.copy(fileTEST_EMPTY_DATABASE, fileTEST_ACCESS_DATABASE);
        /* The JDBC-ODBC bridge could still be used until JAVA 8 using
         * an extract from the JAVA 7 run-time library and the JdbcOdbc.dll.
         * Now that is blocked by the split packages prohibition.
         * So we use the test database originally created under JAVA 8.
         * If we ever want more controlled features in the test database
         * we shall be in trouble ... (have to use JAVA 7 or 8!)
         */
        if (Execute.isOsWindows() && Execute.isJavaVersionLessThan("9"))
            new TestAccessDatabase(fileTEST_ACCESS_DATABASE);
        else FU.copy(fileTEST_ACCESS_SOURCE, fileTEST_ACCESS_DATABASE);
        FU.copy(fileTEST_EMPTY_DATABASE, fileTEST_SQL_DATABASE);
        AccessDataSource dsAccess = new AccessDataSource();
        dsAccess.setDatabaseName(fileTEST_SQL_DATABASE.getAbsolutePath());
        dsAccess.setDescription("SQL data base");
        dsAccess.setReadOnly(false);
        dsAccess.setUser(sUSER);
        dsAccess.setPassword(sPASSWORD);
        AccessConnection connAccess = (AccessConnection) dsAccess.getConnection();
        new TestSqlDatabase(connAccess);
        connAccess.close();
    } /* setUpClass */

    @Before
    public void setUp() {
        try {
            AccessDataSource dsAccess = new AccessDataSource();
            dsAccess.setDatabaseName(fileTEST_SQL_DATABASE.getAbsolutePath());
            dsAccess.setDescription("SQL data base");
            dsAccess.setReadOnly(false);
            dsAccess.setUser(sUSER);
            dsAccess.setPassword(sPASSWORD);
            AccessConnection connAccess = (AccessConnection) dsAccess.getConnection();
            connAccess.setAutoCommit(false);
            setConnection(connAccess);
        } catch (SQLException se) {
            fail(EU.getExceptionMessage(se));
        }
    } /* setUp */

    @Test
    public void testClass() {
        assertEquals("Wrong connection class!", AccessConnection.class, getConnection().getClass());
    } /* testClass */

    @Override
    @Test
    public void testPrepareCall() {
        enter();
        try {
            getConnection().prepareCall(_sSQL);
        } catch (SQLException se) {
            System.out.println(EU.getExceptionMessage(se));
        }
    } /* testPrepareCall*/

    @Override
    @Test
    public void testSetTransactionIsolation() {
        enter();
        try {
            getConnection().setTransactionIsolation(Connection.TRANSACTION_NONE);
        } catch (SQLException se) {
            fail(EU.getExceptionMessage(se));
        }
    } /* testSetTransactionIsolation */

} /* AccessConnectionTester */
