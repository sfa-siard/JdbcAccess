package ch.admin.bar.siard2.jdbc;

import org.junit.runner.*;
import org.junit.runners.*;

@RunWith(Suite.class)
@Suite.SuiteClasses(
  {
    AccessConnectionTester.class,
    AccessDatabaseMetaDataTester.class,
    AccessDriverTester.class,
    AccessResultSetMetaDataTester.class,
    AccessResultSetTester.class,
    AccessStatementTester.class
  })
public class _AccessJdbcTestSuite
{
}
