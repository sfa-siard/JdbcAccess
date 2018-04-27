/*======================================================================
AccessDatabaseMetaData implements wrapped Jackcess DatabaseMetaData for 
MS Access.
Application : Access JDBC driver
Description : AccessDatabaseMetaData implements wrapped Jackcess 
              DatabaseMetaData for MS Access.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 07.11.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.jdbc;

import java.io.*;
import java.nio.charset.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;
import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.query.*;
import ch.enterag.utils.jdbc.*;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.datatype.*;
import ch.enterag.sqlparser.datatype.DataType;
import ch.enterag.sqlparser.datatype.enums.*;
import ch.admin.bar.siard2.access.*;

/*====================================================================*/
/** AccessDatabaseMetaData implements wrapped Jackcess DatabaseMetaData 
 * for MS Access.
 * @author Hartwig Thomas
 */
public class AccessDatabaseMetaData
  extends BaseDatabaseMetaData
  implements DatabaseMetaData
{
  /** schema for metadata result sets */
  private static final String sINFORMATION_SCHEMA = "INFORMATION_SCHEMA";
  /** table name for catalogs result set */
  private static final String sCATALOGS = "CATALOGS";
  /** table name for schemas result set */
  private static final String sSCHEMAS = "SCHEMAS";
  /** table name for tables result set */
  private static final String sTABLES = "TABLES";
  /** table name for table types result set */
  private static final String sTABLE_TYPES = "TABLE_TYPES";
  /** table name for table privileges result set */
  private static final String sTABLE_PRIVILEGES = "TABLE_PRIVILEGES";
  /** table name for columns result set */
  private static final String sCOLUMNS = "COLUMNS";
  /** table name for column privileges result set */
  private static final String sCOLUMN_PRIVILEGES = "COLUMN_PRIVILEGES";
  /** table name for primary keys result set */
  private static final String sPRIMARY_KEYS = "PRIMARY_KEYS";
  /** table name for exported keys result set */
  private static final String sEXPORTED_KEYS = "EXPORTED_KEYS";
  /** table name for imported keys result set */
  private static final String sIMPORTED_KEYS = "IMPORTED_KEYS";
  /** table name for cross reference result set */
  private static final String sCROSS_REFERENCE = "CROSS_REFERENCE";
  /** table name for type info result set */
  private static final String sTYPE_INFO = "TYPE_INFO";
  /** table name for index info result set */
  private static final String sINDEX_INFO = "INDEX_INFO";
  /** table name for functions result set */
  private static final String sFUNCTIONS = "FUNCTIONS";
  /** table name for function columns result set */
  private static final String sFUNCTION_COLUMNS = "FUNCTION_COLUMNS";
  /** table name for procedures result set */
  private static final String sPROCEDURES = "PROCEDURES";
  /** table name for procedure columns result set */
  private static final String sPROCEDURE_COLUMNS = "PROCEDURE_COLUMNS";
  /** table name for super tables result set */
  private static final String sSUPER_TABLES = "SUPER_TABLES";
  /** table name for best row identifier result set */
  private static final String sBEST_ROW_IDENTIFIER = "BEST_ROW_IDENTIFIER";
  /** table name for attributes result set */
  private static final String sATTRIBUTES = "ATTRIBUTES";
  /** table name for super types result set */
  private static final String sSUPER_TYPES = "SUPER_TYPES";
  /** table name for UDTs result set */
  private static final String sUDTS = "UDTS";
  /** table name for CLIENT_INFO result set */
  private static final String sCLIENT_INFO = "CLIENT_INFO";
  /** table name for pseudo columns result set */
  private static final String sPSEUDO_COLUMNS = "PSEUDO_COLUMNS";
  /** table name for version columns result set */
  private static final String sVERSION_COLUMNS = "VERSION_COLUMNS";
  
  /** catalog column */
  private static final String sJDBC_TABLE_CAT = "TABLE_CAT"; 
  /** catalog column */
  private static final String sJDBC_TABLE_CATALOG = "TABLE_CATALOG"; 
  /** schema column */
  private static final String sJDBC_TABLE_SCHEM = "TABLE_SCHEM"; 
  /** table name column */
  private static final String sJDBC_TABLE_NAME = "TABLE_NAME"; 
  /** table type column */
  private static final String sJDBC_TABLE_TYPE = "TABLE_TYPE";
  /** table type TABLE */
  private static final String sJDBC_TABLE_TYPE_TABLE = "TABLE";
  /** table type VIEW */
  private static final String sJDBC_TABLE_TYPE_VIEW = "VIEW";
  /** remarks */
  private static final String sJDBC_REMARKS = "REMARKS"; 
  /** type catalog column */
  private static final String sJDBC_TYPE_CAT = "TYPE_CAT"; 
  /** type schema column */
  private static final String sJDBC_TYPE_SCHEM = "TYPE_SCHEM"; 
  /** type name column */
  private static final String sJDBC_TYPE_NAME = "TYPE_NAME"; 
  /** super type catalog column */
  private static final String sJDBC_SUPERTYPE_CAT = "TYPE_CAT"; 
  /** super type schema column */
  private static final String sJDBC_SUPERTYPE_SCHEM = "TYPE_SCHEM"; 
  /** super type name column */
  private static final String sJDBC_SUPERTYPE_NAME = "TYPE_NAME"; 
  /** self-referencing column name column */
  private static final String sJDBC_SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME"; 
  /** reference generation column */
  private static final String sJDBC_REF_GENERATION = "REF_GENERATION"; 
  /** supertable name column */
  private static final String sJDBC_SUPERTABLE_NAME = "SUPERTABLE_NAME";
  /** grantor column */
  private static final String sJDBC_GRANTOR = "GRANTOR";
  /** grantee column */
  private static final String sJDBC_GRANTEE = "GRANTEE";
  /** privilege column */
  private static final String sJDBC_PRIVILEGE = "PRIVILEGE";
  /** is_grantable column */
  private static final String sJDBC_IS_GRANTABLE = "IS_GRANTABLE";
  /** column name column */
  private static final String sJDBC_COLUMN_NAME = "COLUMN_NAME";
  /** data type column */
  private static final String sJDBC_DATA_TYPE = "DATA_TYPE";
  /** column size column */
  private static final String sJDBC_COLUMN_SIZE = "COLUMN_SIZE";
  /** buffer length column */
  private static final String sJDBC_BUFFER_LENGTH = "BUFFER_LENGTH";
  /** decimal digits column */
  private static final String sJDBC_DECIMAL_DIGITS = "DECIMAL_DIGITS";
  /** radix for numerical precision column */
  private static final String sJDBC_NUM_PREC_RADIX = "NUM_PREC_RADIX";
  /** nullable column */
  private static final String sJDBC_NULLABLE = "NULLABLE";
  /** default value column */
  private static final String sJDBC_COLUMN_DEF = "COLUMN_DEF";
  /** unused SQL data type column */
  private static final String sJDBC_SQL_DATA_TYPE = "SQL_DATA_TYPE";
  /** unused SQL datetime sub type column */
  private static final String sJDBC_SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
  /** byte length column */
  private static final String sJDBC_CHAR_OCTET_LENGTH = "OCTET_LENGTH";
  /** ordinal position column */
  private static final String sJDBC_ORDINAL_POSITION = "ORDINAL_POSITION";
  /** is_nullable column */
  private static final String sJDBC_IS_NULLABLE = "IS_NULLABLE";
  /** scope catalog column */
  private static final String sJDBC_SCOPE_CATALOG = "SCOPE_CATALOG";
  /** scope schema column */
  private static final String sJDBC_SCOPE_SCHEMA = "SCOPE_SCHEMA";
  /** scope table column */
  private static final String sJDBC_SCOPE_TABLE = "SCOPE_TABLE";
  /** source data type column */
  private static final String sJDBC_SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
  /** is_autoincrement column */
  private static final String sJDBC_IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
  /** is_generatedcolumn column */
  private static final String sJDBC_IS_GENERATEDCOLUMN = "IS_GENERATEDCOLUMN";
  /** key sequence column */
  private static final String sJDBC_KEY_SEQ = "KEY_SEQ";
  /** primary key name column */
  private static final String sJDBC_PK_NAME = "PK_NAME";
  /** foreign key name column */
  private static final String sJDBC_FK_NAME = "FK_NAME";
  /** primary key catalog column */
  private static final String sJDBC_PKTABLE_CAT = "PKTABLE_CAT"; 
  /** primary key schema column */
  private static final String sJDBC_PKTABLE_SCHEM = "PKTABLE_SCHEM"; 
  /** primary key table name column */
  private static final String sJDBC_PKTABLE_NAME = "PKTABLE_NAME"; 
  /** primary key column name column */
  private static final String sJDBC_PKCOLUMN_NAME = "PKCOLUMN_NAME"; 
  /** foreign key catalog column */
  private static final String sJDBC_FKTABLE_CAT = "FKTABLE_CAT"; 
  /** foreign key schema column */
  private static final String sJDBC_FKTABLE_SCHEM = "FKTABLE_SCHEM"; 
  /** foreign key table name column */
  private static final String sJDBC_FKTABLE_NAME = "FKTABLE_NAME"; 
  /** foreign key column name column */
  private static final String sJDBC_FKCOLUMN_NAME = "FKCOLUMN_NAME"; 
  /** update rule column */
  private static final String sJDBC_UPDATE_RULE = "UPDATE_RULE"; 
  /** delete rule column */
  private static final String sJDBC_DELETE_RULE = "DELETE_RULE"; 
  /** deferrability column */
  private static final String sJDBC_DEFERRABILITY = "DEFERRABILITY"; 
  /** precision  column */
  private static final String sJDBC_PRECISION = "PRECISION"; 
  /** literal prefix column */
  private static final String sJDBC_LITERAL_PREFIX = "LITERAL_PREFIX"; 
  /** literal suffix column */
  private static final String sJDBC_LITERAL_SUFFIX = "LITERAL_SUFFIX"; 
  /** case-sensitive column */
  private static final String sJDBC_CASE_SENSITIVE = "CASE_SENSITIVE"; 
  /** searchable column */
  private static final String sJDBC_SEARCHABLE = "SEARCHABLE"; 
  /** unsigned attribute column */
  private static final String sJDBC_UNSIGNED_ATTRIBUTE = "UNSIGNED_ATTRIBUTE"; 
  /** fixed precision scale column */
  private static final String sJDBC_FIXED_PREC_SCALE = "FIXED_PREC_SCALE"; 
  /** auto-increment column */
  private static final String sJDBC_AUTOINCREMENT = "AUTOINCREMENT"; 
  /** localized type name column */
  private static final String sJDBC_LOCAL_TYPE_NAME = "LOCAL_TYPE_NAME"; 
  /** minimum scale column */
  private static final String sJDBC_MINIMUM_SCALE = "MINIMUM_SCALE"; 
  /** maximum scale column */
  private static final String sJDBC_MAXIMUM_SCALE = "MAXIMUM_SCALE"; 
  /** non-unique column */
  private static final String sJDBC_NON_UNIQUE = "NON_UNIQUE"; 
  /** index qualifier (catalog) column */
  private static final String sJDBC_INDEX_QUALIFIER = "INDEX_QUALIFIER"; 
  /** index name column */
  private static final String sJDBC_INDEX_NAME = "INDEX_NAME"; 
  /** index type column */
  private static final String sJDBC_TYPE = "TYPE"; 
  /** ascending or descending column */
  private static final String sJDBC_ASC_OR_DESC = "ASC_OR_DESC"; 
  /** cardinality column */
  private static final String sJDBC_CARDINALITY = "CARDINALITY"; 
  /** pages column */
  private static final String sJDBC_PAGES = "PAGES"; 
  /** filter condition column */
  private static final String sJDBC_FILTER_CONDITION = "FILTER_CONDITION"; 
  /** function catalog column */
  private static final String sJDBC_FUNCTION_CAT = "FUNCTION_CAT"; 
  /** function schema column */
  private static final String sJDBC_FUNCTION_SCHEM = "FUNCTION_SCHEM"; 
  /** function name column */
  private static final String sJDBC_FUNCTION_NAME = "FUNCTION_NAME"; 
  /** function type column */
  private static final String sJDBC_FUNCTION_TYPE = "FUNCTION_TYPE"; 
  /** specific name column */
  private static final String sJDBC_SPECIFIC_NAME = "SPECIFIC_NAME"; 
  /** column type column */
  private static final String sJDBC_COLUMN_TYPE = "COLUMN_TYPE"; 
  /** length column */
  private static final String sJDBC_LENGTH = "LENGTH"; 
  /** scale column */
  private static final String sJDBC_SCALE = "SCALE"; 
  /** radix column */
  private static final String sJDBC_RADIX = "RADIX"; 
  /** procedure catalog column */
  private static final String sJDBC_PROCEDURE_CAT = "PROCEDURE_CAT"; 
  /** procedure schema column */
  private static final String sJDBC_PROCEDURE_SCHEM = "PROCEDURE_SCHEM"; 
  /** procedure name column */
  private static final String sJDBC_PROCEDURE_NAME = "PROCEDURE_NAME"; 
  /** procedure type column */
  private static final String sJDBC_PROCEDURE_TYPE = "PROCEDURE_TYPE";
  /** scope column */
  private static final String sJDBC_SCOPE = "SCOPE";
  /** pseudo-column column */
  private static final String sJDBC_PSEUDO_COLUMN = "PSEUDO_COLUMN";
  /** attribute name column */
  private static final String sJDBC_ATTR_NAME = "ATTR_NAME";
  /** attribute type name column */
  private static final String sJDBC_ATTR_TYPE_NAME = "ATTR_TYPE_NAME";
  /** attribute size column */
  private static final String sJDBC_ATTR_SIZE = "ATTR_SIZE";
  /** attribute default column */
  private static final String sJDBC_ATTR_DEF = "ATTR_DEF";
  /** class name column */
  private static final String sJDBC_CLASS_NAME = "CLASS_NAME";
  /** base type column */
  private static final String sJDBC_BASE_TYPE = "BASE_TYPE";
  /** client info name column */
  private static final String sJDBC_NAME = "NAME";
  /** client info maximum length column */
  private static final String sJDBC_MAX_LEN = "MAX_LEN";
  /** client info default value column */
  private static final String sJDBC_DEFAULT_VALUE = "DEFAULT_VALUE";
  /** client info default value column */
  private static final String sJDBC_DESCRIPTION = "DESCRIPTION";
  /** pseudo column usage value column */
  private static final String sJDBC_COLUMN_USAGE = "COLUMN_USAGE";

  /** reserved column */
  private static final String sJDBC_RESERVED1 = "RESERVED1"; 
  /** reserved column */
  private static final String sJDBC_RESERVED2 = "RESERVED2"; 
  /** reserved column */
  private static final String sJDBC_RESERVED3 = "RESERVED3"; 
  
  /** database connection */
  private AccessConnection _conn = null;
  /** map from query name to query */
  private Map<String,SelectQuery> _mapViews = null;

  /*------------------------------------------------------------------*/
  /** checks, whether string value matches the JDBC pattern 
   * (with _ for any character and % for any string).
   * @param sPattern JDBC pattern.
   * @param sValue string value.
   * @return true, if sValue matches sPattern.
   */
  static boolean matches(String sPattern, String sValue)
  {
    boolean bMatch = true;
    if (sPattern != null)
    {
      /* escape all regex special characters
       * ".", "*", "|", "?", "+", "(", ")", "[", "]", "{", "}", "^", "$", "\" 
       * ("\" first!) */
      String sMetaCharacters = "$^}{][)(+?|*.";
      for (int i = 0; i < sMetaCharacters.length(); i++)
      {
        String sCharEscape = sMetaCharacters.substring(i,i+1);
        sPattern = sPattern.replace(sCharEscape, "\\"+sCharEscape);
      }
      /* neither '_' nor '%' are special characters for JAVA regular expressions */
      sPattern = sPattern.replaceAll("^_", ".").replaceAll("([^\\\\])_", "$1.").
        replaceAll("^%", ".*").replaceAll("([^\\\\])%", "$1.*");
      Pattern pattern = Pattern.compile(sPattern);
      Matcher matcher = pattern.matcher(sValue);
      bMatch = matcher.matches();
    }
    return bMatch;
  } /* matches */
  
   /*------------------------------------------------------------------*/
  /** constructor stores connection to database. 
   * @param conn database connection.
   * @param session H2 session.
   * @throws SQLException, if the view map could not be established.
   */
  AccessDatabaseMetaData(AccessConnection conn) throws SQLException
  {
    super(null);
    _conn = conn;
    /* create view map */
    _mapViews = new HashMap<String,SelectQuery>();
    try
    {
      List<Query> listQuery = _conn.getDatabase().getQueries();
      for (Iterator<Query> iterQuery = listQuery.iterator(); iterQuery.hasNext(); )
      {
        Query query = iterQuery.next();
        if (query.getType().equals(Query.Type.SELECT))
          _mapViews.put(query.getName(), (SelectQuery)query);
      }
    }
    catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
  } /* constructor AccessDatabaseMetaData */

  /*====================================================================
  Wrapper 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean isWrapperFor(Class<?> clsInterface) throws SQLException
  {
    return clsInterface.equals(DatabaseMetaData.class);
  } /* isWrapperFor */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> clsInterface) throws SQLException
  {
    T impl = null;
    if (isWrapperFor(clsInterface))
      impl = (T)this;
    else
      throw new IllegalArgumentException("AccessDatabaseMetaData cannot be unwrapped to "+clsInterface.getName()+"!");
    return impl;
  } /* unwrap */
  
  /*====================================================================
  Database properties 
  ====================================================================*/
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean allProceduresAreCallable() throws SQLException
  {
    return false;
  } /* allProceduresAreCallable */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean allTablesAreSelectable() throws SQLException
  {
    return true;
  } /* allTablesAreSelectable */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean autoCommitFailureClosesAllResultSets()
      throws SQLException
  {
    return false;
  } /* autoCommitFailureClosesAllResultSets */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean dataDefinitionCausesTransactionCommit()
      throws SQLException
  {
    return true;
  } /* dataDefinitionCausesTransactionCommit */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean dataDefinitionIgnoredInTransactions()
      throws SQLException
  {
    return false;
  } /* dataDefinitionIgnoredInTransactions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean deletesAreDetected(int arg0) throws SQLException
  {
    //  TODO: check, whether ResultSet.rowDeleted is serviced  
    return false;
  } /* deletesAreDetected */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean insertsAreDetected(int arg0) throws SQLException
  {
    //  TODO: check, whether ResultSet.rowInserted is serviced  
    return false;
  } /* insertsAreDetected */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean updatesAreDetected(int arg0) throws SQLException
  {
    //  TODO: check, whether ResultSet.rowUpdated is serviced  
    return false;
  } /* updatesAreDetected */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
  {
    // TODO: Maximum is 4 K excluding large fields
    return false;
  } /* doesMaxRowSizeIncludeBlobs */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} for JDK 1.7 */
  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException
  {
    return false;
  } /* generatedKeyAlwaysReturned */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getDefaultTransactionIsolation() throws SQLException
  {
    return Connection.TRANSACTION_NONE;
  } /* getDefaultTransactionIsolation */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of ClientInfo columns */
  class ClientInfoComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
        row1.getString(sJDBC_NAME),
        row2.getString(sJDBC_NAME)
      );
      return iCompare;
    } /* compare */
  } /* ClientInfoComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getClientInfoProperties() throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sCLIENT_INFO);
    rsh.addColumn(sJDBC_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_MAX_LEN, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_DEFAULT_VALUE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DESCRIPTION, java.sql.Types.VARCHAR);
    List<Row> listClientInfo = new ArrayList<Row>();
    Collections.sort(listClientInfo,new ClientInfoComparator());
    MetaDataCursor mdc = new MetaDataCursor(listClientInfo);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getClientInfoProperties */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxBinaryLiteralLength() throws SQLException
  {
    return 510;
  } /* getMaxBinaryLiteralLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxCatalogNameLength() throws SQLException
  {
    return 0;
  } /* getMaxCatalogNameLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxCharLiteralLength() throws SQLException
  {
    return 255;
  } /* getMaxCharLiteralLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxColumnNameLength() throws SQLException
  {
    return 64;
  } /* getMaxColumnNameLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxColumnsInGroupBy() throws SQLException
  {
    return 255;
  } /* getMaxColumnsInGroupBy */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxColumnsInIndex() throws SQLException
  {
    return 10;
  } /* getMaxColumnsInIndex */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxColumnsInOrderBy() throws SQLException
  {
    return 255;
  } /* getMaxColumnsInIndex */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxColumnsInSelect() throws SQLException
  {
    return 255;
  } /* getMaxColumnsInSelect */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxColumnsInTable() throws SQLException
  {
    return 255;
  } /* getMaxColumnsInTable */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxConnections() throws SQLException
  {
    return 255;
  } /* getMaxConnections */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxCursorNameLength() throws SQLException
  {
    return 64;
  } /* getMaxCursorNameLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxIndexLength() throws SQLException
  {
    return 0;
  } /* getMaxIndexLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxProcedureNameLength() throws SQLException
  {
    return 64;
  } /* getMaxProcedureNameLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxRowSize() throws SQLException
  {
    return 4000;
  } /* getMaxRowSize */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxSchemaNameLength() throws SQLException
  {
    return 20;
  } /* getMaxSchemaNameLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxStatementLength() throws SQLException
  {
    return 64000;
  } /* getMaxStatementLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxStatements() throws SQLException
  {
    return 0;
  } /* getMaxStatements */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxTableNameLength() throws SQLException
  {
    return 64;
  } /* getMaxTableNameLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxTablesInSelect() throws SQLException
  {
    return 32;
  } /* getMaxTablesInSelect */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getMaxUserNameLength() throws SQLException
  {
    return 20;
  } /* getMaxUserNameLength */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public Connection getConnection() throws SQLException
  {
    return _conn;
  } /* getConnection */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getDatabaseProductName() throws SQLException
  {
    String sDatabaseProductName = "MS Access";
    try { sDatabaseProductName = sDatabaseProductName + " "+_conn.getDatabase().getFileFormat().toString();  }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return sDatabaseProductName;
  } /* getDatabaseProductName */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getDatabaseProductVersion() throws SQLException
  {
    String sVersion = null;
    try 
    { 
      String sAccessVersion = (String)_conn.getDatabase().getDatabaseProperties().getValue("AccessVersion");
      Integer iBuild = (Integer)_conn.getDatabase().getDatabaseProperties().getValue("Build");
      sVersion = sAccessVersion+"."+String.valueOf(iBuild);
    }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return sVersion;
  } /* getDatabaseProductVersion */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getDatabaseMajorVersion() throws SQLException
  {
    int iMajorVersion = -1;
    String sAccessVersion = getDatabaseProductVersion();
    if (sAccessVersion != null)
    {
      String[] asVersion = sAccessVersion.split("\\.");
      if (asVersion.length > 0)
        iMajorVersion = Integer.parseInt(asVersion[0]);
    }
    return iMajorVersion;
  } /* getDatabaseMajorVersion */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getDatabaseMinorVersion() throws SQLException
  {
    int iMinorVersion = -1;
    String sAccessVersion = getDatabaseProductVersion();
    if (sAccessVersion != null)
    {
      String[] asVersion = sAccessVersion.split("\\.");
      if (asVersion.length > 1)
        iMinorVersion = Integer.parseInt(asVersion[1]);
    }
    return iMinorVersion;
  } /* getDatabaseMinorVersion */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getDriverVersion() throws SQLException
  {
    return AccessDriver.getVersion();
  } /* getDriverVersion */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getDriverMajorVersion()
  {
    int iMajorVersion = -1;
    String sVersion = AccessDriver.getVersion();
    String[] asVersion = sVersion.split("\\.");
    if (asVersion.length > 0)
      iMajorVersion = Integer.parseInt(asVersion[0]);
    return iMajorVersion;
  }

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getDriverMinorVersion()
  {
    int iMinorVersion = -1;
    String sVersion = AccessDriver.getVersion();
    String[] asVersion = sVersion.split("\\.");
    if (asVersion.length > 1)
      iMinorVersion = Integer.parseInt(asVersion[1]);
    return iMinorVersion;
  }

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getDriverName() throws SQLException
  {
    String sName = AccessDriver.class.getName();
    String[] asName = sName.split("\\.");
    sName = asName[asName.length-1];
    return sName;
  } /* getDriverName */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getJDBCMajorVersion() throws SQLException
  {
    return 4;
  } /* getJDBCMajorVersion */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getJDBCMinorVersion() throws SQLException
  {
    return 0;
  } /* getJDBCMinorVersion */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getCatalogSeparator() throws SQLException
  {
    return ".";
  } /* getCatalogSeparator */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getSearchStringEscape() throws SQLException
  {
    return "\\";
  } /* getSearchStringEscape */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getCatalogTerm() throws SQLException
  {
    return "catalog";
  } /* getCatalogTerm */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getSchemaTerm() throws SQLException
  {
    return "schema";
  } /* getSchemaTerm */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getProcedureTerm() throws SQLException
  {
    return "procedure";
  } /* getProcedureTerm */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getUserName() throws SQLException
  {
    return _conn.getUserName();
  } /* getUserName */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getExtraNameCharacters() throws SQLException
  {
    return "";
  } /* getExtraNameCharacters */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getIdentifierQuoteString() throws SQLException
  {
    return "\"";
  } /* getIdentifierQuoteString */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getResultSetHoldability() throws SQLException
  {
    return _conn.getHoldability();
  } /* getResultSetHoldability */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException
  {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  } /* getRowIdLifetime */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public int getSQLStateType() throws SQLException
  {
    return DatabaseMetaData.sqlStateSQL;
  } /* getSQLStateType */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public String getURL() throws SQLException
  {
    return AccessDriver.getUrl(_conn.getDatabase().getFile().getAbsolutePath());
  } /* getURL */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * currently no SQL parsing supported */
  @Override
  public String getNumericFunctions() throws SQLException
  {
    return "";
  } /* getNumericFunctions */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * should really be expanded ... */
  @Override
  public String getSQLKeywords() throws SQLException
  {
    return "DELETE, INSERT, SELECT, UPDATE";
  } /* getSQLKeywords */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * currently no SQL parsing supported */
  @Override
  public String getStringFunctions() throws SQLException
  {
    return "";
  } /* getStringFunctions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * currently no SQL parsing supported */
  @Override
  public String getSystemFunctions() throws SQLException
  {
    return "";
  } /* getSystemFunctions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * currently no SQL parsing supported */
  @Override
  public String getTimeDateFunctions() throws SQLException
  {
    return "";
  } /* getTimeDateFunctions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean isCatalogAtStart() throws SQLException
  {
    return false;
  } /* isCatalogAtStart */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean isReadOnly() throws SQLException
  {
    return _conn.isReadOnly();
  } /* isReadOnly */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean locatorsUpdateCopy() throws SQLException
  {
    return false;
  } /* locatorsUpdateCopy */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException
  {
    return true;
  } /* nullPlusNonNullIsNull */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException
  {
    return false;
  } /* nullsAreSortedAtEnd */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean nullsAreSortedAtStart() throws SQLException
  {
    return false;
  } /* nullsAreSortedAtStart */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean nullsAreSortedHigh() throws SQLException
  {
    return false;
  } /* nullsAreSortedHigh */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean nullsAreSortedLow() throws SQLException
  {
    return true;
  } /* nullsAreSortedLow */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean othersDeletesAreVisible(int iResultSetType) throws SQLException
  {
    return true;
  } /* othersDeletesAreVisible */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean othersInsertsAreVisible(int iResultSetType) throws SQLException
  {
    return true;
  } /* othersInsertsAreVisible */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean othersUpdatesAreVisible(int iResultSetType) throws SQLException
  {
    return true;
  } /* othersUpdatesAreVisible */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean ownDeletesAreVisible(int iResultSetType) throws SQLException
  {
    return true;
  } /* ownDeletesAreVisible */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean ownInsertsAreVisible(int iResultSetType) throws SQLException
  {
    return true;
  } /* ownInsertsAreVisible */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean ownUpdatesAreVisible(int iResultSetType) throws SQLException
  {
    return true;
  } /* ownUpdatesAreVisible */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException
  {
    return false;
  } /* storesLowerCaseIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
  {
    return false;
  } /* storesLowerCaseQuotedIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException
  {
    return true;
  } /* storesMixedCaseIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
  {
    return true;
  } /* storesMixedCaseQuotedIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException
  {
    return false;
  } /* storesUpperCaseIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
  {
    return false;
  } /* storesUpperCaseQuotedIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException
  {
    return false;
  } /* supportsANSI92EntryLevelSQL */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsANSI92FullSQL() throws SQLException
  {
    return false;
  } /* supportsANSI92FullSQL */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException
  {
    return false;
  } /* supportsANSI92IntermediateSQL */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not support table altering yet ... */
  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException
  {
    return false;
  } /* supportsAlterTableWithAddColumn */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not support table altering yet ... */
  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException
  {
    return false;
  } /* supportsAlterTableWithDropColumn */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsBatchUpdates() throws SQLException
  {
    return false;
  } /* supportsBatchUpdates */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsCatalogsInDataManipulation()
      throws SQLException
  {
    return false;
  } /* supportsCatalogsInDataManipulation */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsCatalogsInIndexDefinitions()
      throws SQLException
  {
    return false;
  } /* supportsCatalogsInIndexDefinitions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions()
      throws SQLException
  {
    return false;
  } /* supportsCatalogsInPrivilegeDefinitions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException
  {
    return false;
  } /* supportsCatalogsInProcedureCalls */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsCatalogsInTableDefinitions()
      throws SQLException
  {
    return false;
  } /* supportsCatalogsInTableDefinitions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsColumnAliasing() throws SQLException
  {
    return false;
  } /* supportsColumnAliasing */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsConvert() throws SQLException
  {
    return false;
  } /* supportsConvert */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsConvert(int iFromType, int iToType)
      throws SQLException
  {
    return false;
  } /* supportsConvert */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException
  {
    return false;
  } /* supportsCoreSQLGrammar */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException
  {
    return false;
  } /* supportsCorrelatedSubqueries */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not support transactions yet ... */
  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException
  {
    return false;
  } /* supportsDataDefinitionAndDataManipulationTransactions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not support transactions yet ... */
  @Override
  public boolean supportsDataManipulationTransactionsOnly()
      throws SQLException
  {
    return false;
  } /* supportsDataManipulationTransactionsOnly */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsDifferentTableCorrelationNames()
      throws SQLException
  {
    return false;
  } /* supportsDifferentTableCorrelationNames */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException
  {
    return false;
  } /* supportsExpressionsInOrderBy */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException
  {
    return false;
  } /* supportsExtendedSQLGrammar */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsFullOuterJoins() throws SQLException
  {
    return false;
  } /* supportsFullOuterJoins */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException
  {
    return false;
  } /* supportsGetGeneratedKeys */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsGroupBy() throws SQLException
  {
    return false;
  } /* supportsGroupBy */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException
  {
    return false;
  } /* supportsGroupByBeyondSelect */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsGroupByUnrelated() throws SQLException
  {
    return false;
  } /* supportsGroupByUnrelated */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsIntegrityEnhancementFacility()
      throws SQLException
  {
    return false;
  } /* supportsIntegrityEnhancementFacility */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsLikeEscapeClause() throws SQLException
  {
    return false;
  } /* supportsLikeEscapeClause */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException
  {
    return false;
  } /* supportsLimitedOuterJoins */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException
  {
    return false;
  } /* supportsMinimumSQLGrammar */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException
  {
    return true;
  } /* supportsMixedCaseIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsMixedCaseQuotedIdentifiers()
      throws SQLException
  {
    return true;
  } /* supportsMixedCaseQuotedIdentifiers */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * No callable statements ... */
  @Override
  public boolean supportsMultipleOpenResults() throws SQLException
  {
    return false;
  } /* supportsMultipleOpenResults */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsMultipleResultSets() throws SQLException
  {
    return true;
  } /* supportsMultipleResultSets */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * No transactions */
  @Override
  public boolean supportsMultipleTransactions() throws SQLException
  {
    return false;
  } /* supportsMultipleTransactions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * No callable statements ... */
  @Override
  public boolean supportsNamedParameters() throws SQLException
  {
    return false;
  } /* supportsNamedParameters */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsNonNullableColumns() throws SQLException
  {
    return true;
  } /* supportsNonNullableColumns */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * No transactions */
  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException
  {
    return true;
  } /* supportsOpenCursorsAcrossCommit */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * No transactions */
  @Override
  public boolean supportsOpenCursorsAcrossRollback()
      throws SQLException
  {
    return false;
  } /* supportsOpenCursorsAcrossRollback */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * No transactions */
  @Override
  public boolean supportsOpenStatementsAcrossCommit()
      throws SQLException
  {
    return true;
  } /* supportsOpenStatementsAcrossCommit */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * No transactions */
  @Override
  public boolean supportsOpenStatementsAcrossRollback()
      throws SQLException
  {
    return false;
  } /* supportsOpenStatementsAcrossRollback */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsOrderByUnrelated() throws SQLException
  {
    return false;
  } /* supportsOrderByUnrelated */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsOuterJoins() throws SQLException
  {
    return false;
  } /* supportsOuterJoins */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsPositionedDelete() throws SQLException
  {
    return true;
  } /* supportsPositionedDelete */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsPositionedUpdate() throws SQLException
  {
    return true;
  } /* supportsPositionedUpdate */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsResultSetConcurrency(int iType, int iConcurrency)
      throws SQLException
  {
    return (iConcurrency == ResultSet.CONCUR_READ_ONLY);
  }

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsResultSetHoldability(int iHoldability)
      throws SQLException
  {
    return (iHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT);
  } /* supportsResultSetHoldability */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsResultSetType(int iType) throws SQLException
  {
    return true;
  } /* supportsResultSetType */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSavepoints() throws SQLException
  {
    return true;
  } /* supportsSavepoints */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSchemasInDataManipulation()
      throws SQLException
  {
    return false;
  } /* supportsSchemasInDataManipulation */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSchemasInIndexDefinitions()
      throws SQLException
  {
    return false;
  } /* supportsSchemasInIndexDefinitions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSchemasInPrivilegeDefinitions()
      throws SQLException
  {
    return false;
  } /* supportsSchemasInPrivilegeDefinitions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException
  {
    return false;
  } /* supportsSchemasInProcedureCalls */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSchemasInTableDefinitions()
      throws SQLException
  {
    return false;
  } /* supportsSchemasInTableDefinitions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * we do not have a decent SQL parser yet ... */
  @Override
  public boolean supportsSelectForUpdate() throws SQLException
  {
    return false;
  } /* supportsSelectForUpdate */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsStatementPooling() throws SQLException
  {
    return false;
  } /* supportsStatementPooling */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax()
      throws SQLException
  {
    return false;
  } /* supportsStoredFunctionsUsingCallSyntax */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsStoredProcedures() throws SQLException
  {
    return false;
  } /* supportsStoredProcedures */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException
  {
    return false;
  } /* supportsSubqueriesInComparisons */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSubqueriesInExists() throws SQLException
  {
    return false;
  } /* supportsSubqueriesInExists */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSubqueriesInIns() throws SQLException
  {
    return false;
  } /* supportsSubqueriesInIns */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException
  {
    return false;
  } /* supportsSubqueriesInQuantifieds */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsTableCorrelationNames() throws SQLException
  {
    return false;
  } /* supportsTableCorrelationNames */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsTransactionIsolationLevel(int iLevel)
      throws SQLException
  {
    return (iLevel == Connection.TRANSACTION_NONE);
  } /* supportsTransactionIsolationLevel */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsTransactions() throws SQLException
  {
    return false;
  } /* supportsTransactions */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsUnion() throws SQLException
  {
    return false;
  } /* supportsUnion */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean supportsUnionAll() throws SQLException
  {
    return false;
  } /* supportsUnionAll */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean usesLocalFilePerTable() throws SQLException
  {
    return false;
  } /* usesLocalFilePerTable */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public boolean usesLocalFiles() throws SQLException
  {
    return true;
  } /* usesLocalFiles */
  
  /*====================================================================
  Meta data result sets 
  ====================================================================*/
  /** Compares two string, handling nulls.
   * @param s1 first string.
   * @param s2 second string.
   * @return -1 if first string is before second string, 
   *          0 if they are equal, 
   *          1 if first string is after second string
   */
  static int compareStrings(String s1, String s2)
  {
    int iCompare = 0;
    if ((s1 != null) && (s2 != null))
      iCompare = s1.compareTo(s2);
    else if ((s1 == null) && (s2 != null))
      iCompare = -1;
    else if ((s1 != null) && (s2 == null))
      iCompare = 1;
    return iCompare;
  } /* compareStrings */
  
  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of catalogs */
  class CatalogsComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      String sCatalog1 = row1.getString(sJDBC_TABLE_CAT);
      String sCatalog2 = row2.getString(sJDBC_TABLE_CAT);
      return compareStrings(sCatalog1,sCatalog2);
    } /* compare */
  } /* CatalogsComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getCatalogs() throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sCATALOGS);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    List<Row> listCatalogs = new ArrayList<Row>();
    Collections.sort(listCatalogs, new CatalogsComparator());
    MetaDataCursor mdc = new MetaDataCursor(listCatalogs);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getCatalogs */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of schemas */
  class SchemasComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = 0;
      iCompare = compareStrings(
          row1.getString(sJDBC_TABLE_CATALOG), 
          row2.getString(sJDBC_TABLE_CATALOG));
      if (iCompare == 0)
      {
        iCompare = compareStrings(
            row1.getString(sJDBC_TABLE_SCHEM),
            row2.getString(sJDBC_TABLE_SCHEM));
      }
      return iCompare;
    } /* compare */
  } /* SchemasComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * returns set with single schema identical to user */
  @Override
  public ResultSet getSchemas() throws SQLException
  {
    return getSchemas(null,null);
  } /* getSchemas */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getSchemas(String sCatalog, String sSchemaPattern)
      throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sSCHEMAS);
    rsh.addColumn(sJDBC_TABLE_CATALOG, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    List<Row> listSchemas = new ArrayList<Row>();
    if ((sCatalog == null) || sCatalog.equals(_conn.getCatalog()))
    {
      /* single schema: use user name */
      String sSchemaName = getUserName();
      if (matches(sSchemaPattern,sSchemaName))
      {
        ResultSetRow row = new ResultSetRow();
        row.put(sJDBC_TABLE_CATALOG, sCatalog);
        row.put(sJDBC_TABLE_SCHEM, sSchemaName);
        listSchemas.add(row);
      }
    }
    Collections.sort(listSchemas,new SchemasComparator());
    MetaDataCursor mdc = new MetaDataCursor(listSchemas);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getSchemas */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} 
   * returns TABLE and VIEW */
  @Override
  public ResultSet getTableTypes() throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sTABLE_TYPES);
    rsh.addColumn(sJDBC_TABLE_TYPE, java.sql.Types.VARCHAR);
    List<Row> listTableTypes = new ArrayList<Row>();
    ResultSetRow row = new ResultSetRow();
    row.put(sJDBC_TABLE_TYPE, sJDBC_TABLE_TYPE_TABLE);
    listTableTypes.add(row);
    row = new ResultSetRow();
    row.put(sJDBC_TABLE_TYPE, sJDBC_TABLE_TYPE_VIEW);
    listTableTypes.add(row);
    MetaDataCursor mdc = new MetaDataCursor(listTableTypes);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getTableTypes */

  /*------------------------------------------------------------------*/
  /** Compares three sets of strings 
   * @param sCatalog1 catalog of first set.
   * @param sSchema1 schema of first set.
   * @param sTable1 table of first set.
   * @param sCatalog2 catalog of second set.
   * @param sSchema2 schema of second set.
   * @param sTable2 table of second set.
   * @return -1 if first set is before second set, 
   *          0, if they are equal, 
   *          1 if first set is after second set.
   */
  int compareStrings(
      String sCatalog1,
      String sSchema1, 
      String sTable1, 
      String sCatalog2,
      String sSchema2,
      String sTable2)
  {
    int iCompare = compareStrings(sCatalog1, sCatalog2);
    if (iCompare == 0)
      iCompare = compareStrings(sSchema1, sSchema2);
    if (iCompare == 0)
      iCompare = compareStrings(sTable1, sTable2);
    return iCompare;
  } /* compareStrings */
  
  /*------------------------------------------------------------------*/
  /** compare four sets of strings 
   * @param sCatalog1 catalog of first set.
   * @param sSchema1 schema of first set.
   * @param sTable1 table of first set.
   * @param sAttribute1 attribute (e.g. column) of first set.
   * @param sCatalog2 catalog of second set.
   * @param sSchema2 schema of second set.
   * @param sTable2 table of second set.
   * @param sAttribute2 attribute (e.g. column) of second set.
   * @return -1 if first set is before second set, 
   *          0 if they are equal, 
   *          1 if first set is after second set.
   */
  int compareStrings(
      String sCatalog1,
      String sSchema1, 
      String sTable1, 
      String sAttribute1,
      String sCatalog2,
      String sSchema2,
      String sTable2,
      String sAttribute2)
  {
    int iCompare = compareStrings(
        sCatalog1, 
        sSchema1, 
        sTable1, 
        sCatalog2,
        sSchema2, 
        sTable2);
    if (iCompare == 0)
      iCompare = compareStrings(sAttribute1, sAttribute2);
    return iCompare;
  } /* compareStrings */
  
  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of tables */
  class TablesComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
          row1.getString(sJDBC_TABLE_TYPE),
          row1.getString(sJDBC_TABLE_CAT),
          row1.getString(sJDBC_TABLE_SCHEM),
          row1.getString(sJDBC_TABLE_NAME),
          row2.getString(sJDBC_TABLE_TYPE),
          row2.getString(sJDBC_TABLE_CAT),
          row2.getString(sJDBC_TABLE_SCHEM),
          row2.getString(sJDBC_TABLE_NAME));
      return iCompare;
    } /* compare */
  } /* TablesComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getTables(String sCatalog, String sSchemaPattern, 
      String sTableNamePattern, String[] asTypes) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sTABLES);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_TYPE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SELF_REFERENCING_COL_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_REF_GENERATION, java.sql.Types.VARCHAR);
    List<Row> listTables = new ArrayList<Row>();
    if ((sCatalog == null) || sCatalog.equals(_conn.getCatalog()))
    {
      String sSchemaName = getUserName();
      if (matches(sSchemaPattern,sSchemaName))
      {
        try
        {
          Set<String> setTypes = new HashSet<String>();
          if (asTypes != null)
            setTypes = new HashSet<String>(Arrays.asList(asTypes));
          if ((asTypes == null) || (setTypes.contains(sJDBC_TABLE_TYPE_TABLE)))
          {
            /* find all matching tables */
            Set<String> setTableNames = _conn.getDatabase().getTableNames();
            for (Iterator<String> iterTableName = setTableNames.iterator(); iterTableName.hasNext(); )
            {
              String sTableName = iterTableName.next();
              if (matches(sTableNamePattern, sTableName))
              {
                ResultSetRow row = new ResultSetRow();
                row.put(sJDBC_TABLE_CAT, sCatalog);
                row.put(sJDBC_TABLE_SCHEM, sSchemaName);
                row.put(sJDBC_TABLE_NAME, sTableName);
                row.put(sJDBC_TABLE_TYPE, sJDBC_TABLE_TYPE_TABLE);
                Table table = _conn.getDatabase().getTable(sTableName);
                /* table still may be in set, although it was dropped ... */
                if (table != null)
                {
                  String sDescription = null;
                  PropertyMap pm = table.getProperties();
                  if (pm != null)
                    sDescription = (String)pm.getValue("Description");
                  row.put(sJDBC_REMARKS, sDescription);
                  row.put(sJDBC_TYPE_CAT,null);
                  row.put(sJDBC_TYPE_SCHEM,null);
                  row.put(sJDBC_TYPE_NAME,null);
                  row.put(sJDBC_SELF_REFERENCING_COL_NAME,null);
                  row.put(sJDBC_REF_GENERATION,null);
                  listTables.add(row);
                }
              }
            }
          }
          if ((asTypes == null) || (setTypes.contains("VIEW")))
          {
            /* find all matching views */
            for (Iterator<String> iterView = _mapViews.keySet().iterator(); iterView.hasNext(); )
            {
              String sViewName = iterView.next();
              if (matches(sTableNamePattern, sViewName))
              {
                SelectQuery sq = _mapViews.get(sViewName);
                ResultSetRow row = new ResultSetRow();
                row.put(sJDBC_TABLE_CAT, sCatalog);
                row.put(sJDBC_TABLE_SCHEM, sSchemaName);
                row.put(sJDBC_TABLE_NAME, sq.getName());
                row.put(sJDBC_TABLE_TYPE, sJDBC_TABLE_TYPE_VIEW);
                row.put(sJDBC_REMARKS, sq.toSQLString());
                row.put(sJDBC_TYPE_CAT,null);
                row.put(sJDBC_TYPE_SCHEM,null);
                row.put(sJDBC_TYPE_NAME,null);
                row.put(sJDBC_SELF_REFERENCING_COL_NAME,null);
                row.put(sJDBC_REF_GENERATION,null);
                listTables.add(row);
              }
            }
          }
        }
        catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
      }
    }
    Collections.sort(listTables,new TablesComparator());
    MetaDataCursor mdc = new MetaDataCursor(listTables);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getTables */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of table privileges */
  class PrivilegesComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
          row1.getString(sJDBC_TABLE_CAT),
          row1.getString(sJDBC_TABLE_SCHEM),
          row1.getString(sJDBC_TABLE_NAME),
          row1.getString(sJDBC_PRIVILEGE),
          row2.getString(sJDBC_TABLE_CAT),
          row2.getString(sJDBC_TABLE_SCHEM),
          row2.getString(sJDBC_TABLE_NAME),
          row2.getString(sJDBC_PRIVILEGE));
      return iCompare;
    } /* compare */
  } /* PrivilegesComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getTablePrivileges(String sCatalog, String sSchemaPattern, String sTablePattern) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sTABLE_PRIVILEGES);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_GRANTOR, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_GRANTEE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PRIVILEGE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_IS_GRANTABLE, java.sql.Types.VARCHAR);
    List<Row> listTablePrivileges = new ArrayList<Row>();
    Collections.sort(listTablePrivileges,new PrivilegesComparator());
    MetaDataCursor mdc = new MetaDataCursor(listTablePrivileges);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getTablePrivileges */

  /*------------------------------------------------------------------*/
  /** compare three sets of strings and an int 
   * @param sCatalog1
   * @param sSchema1
   * @param sTable1
   * @param iColumn1
   * @param sCatalog2
   * @param sSchema2
   * @param sTable2
   * @param iColumn2
   * @return -1 if first set is before second set, 0, if they are equal, 1 if first set is after second set.
   */
  int compareColumns(
      String sCatalog1,
      String sSchema1, 
      String sTable1, 
      Integer iColumn1,
      String sCatalog2,
      String sSchema2,
      String sTable2,
      Integer iColumn2)
  {
    int iCompare = compareStrings(
        sCatalog1, 
        sSchema1, 
        sTable1, 
        sCatalog2,
        sSchema2, 
        sTable2);
    if (iCompare == 0)
      iCompare = iColumn1.compareTo(iColumn2);
    return iCompare;
  } /* compareColumns */
  
  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of columns */
  class ColumnsComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareColumns(
          row1.getString(sJDBC_TABLE_CAT),
          row1.getString(sJDBC_TABLE_SCHEM),
          row1.getString(sJDBC_TABLE_NAME),
          row1.getInt(sJDBC_ORDINAL_POSITION),
          row2.getString(sJDBC_TABLE_CAT),
          row2.getString(sJDBC_TABLE_SCHEM),
          row2.getString(sJDBC_TABLE_NAME),
          row2.getInt(sJDBC_ORDINAL_POSITION));
      return iCompare;
    } /* compare */
  } /* ColumnsComparator */
  
  /*------------------------------------------------------------------*/
  /** return an SQL query for the SelectQuery
   * @param sq select query.
   * @return SQL query.
   */
  private String getQuery(SelectQuery sq)
  {
    StringBuilder sbSql = new StringBuilder("SELECT ");
    List<String> listColumns = sq.getSelectColumns();
    for (int iColumn = 0; iColumn < listColumns.size(); iColumn++)
    {
      if (iColumn > 0)
        sbSql.append(",");
      sbSql.append("\r\n  ");
      // TODO: should really be evaluated as a value expression!
      String sColumnName = listColumns.get(iColumn); 
      sbSql.append(SqlLiterals.formatId(sColumnName));
    }
    sbSql.append("\r\nFROM ");
    List<String> listTables = sq.getFromTables();
    for (int iTable = 0; iTable < listTables.size(); iTable++)
    {
      if (iTable > 0)
        sbSql.append(", ");
      // TODO: should really be evaluated as a table expression!
      String sTableName = listTables.get(iTable);
      sbSql.append(SqlLiterals.formatId(sTableName));
    }
    String sWhereExpression = sq.getWhereExpression();
    if ((sWhereExpression != null) && (sWhereExpression.length() > 0))
    {
      sbSql.append("\r\nWHERE ");
      // TODO: should really be evaluated as a boolean value expression!
      sbSql.append(sWhereExpression);
    }
    List<String> listGroupings = sq.getGroupings();
    if ((listGroupings != null) && (listGroupings.size() > 0))
    {
      sbSql.append("\r\nGROUP BY ");
      for (int iGrouping = 0; iGrouping < listGroupings.size(); iGrouping++)
      {
        if (iGrouping > 0)
          sbSql.append(", ");
        String sGrouping = listGroupings.get(iGrouping);
        // TODO: should really be evaluated as a value expression!
        sbSql.append(SqlLiterals.formatId(sGrouping));
      }
    }
    String sHavingExpression = sq.getHavingExpression();
    if ((sHavingExpression != null) && (sHavingExpression.length() > 0))
    {
      sbSql.append("\r\nHAVING ");
      // TODO: should really be evaluated as a boolean value expression!
      sbSql.append(sHavingExpression);
    }
    List<String> listOrderings = sq.getOrderings();
    if ((listOrderings != null) && (listOrderings.size() > 0))
    {
      sbSql.append("\r\nORDER BY ");
      for (int iOrdering = 0; iOrdering < listOrderings.size(); iOrdering++)
      {
        if (iOrdering > 0)
          sbSql.append(", ");
        String sOrdering = listOrderings.get(iOrdering);
        // TODO: should really be evaluated as a value expression!
        sbSql.append(SqlLiterals.formatId(sOrdering));
      }
    }
    return sbSql.toString();
  }
  
  /*------------------------------------------------------------------*/
  /** create and fill a row of JDBC column description from the
   * Jackcess column object.
   * @param sCatalog catalog.
   * @param sSchema schema.
   * @param sTableName table name.
   * @param sColumnName (alias) name of the column.
   * @param column Jackcess column object.
   * @return row of JDBC column description.
   * @throws SQLException if an I/O error occurred.
   */
  private ResultSetRow getColumnRow(String sCatalog, String sSchema, 
    String sTableName, String sColumnName, Column column) throws SQLException
  {
    ResultSetRow row = new ResultSetRow();
    try
    {
      int iScale = column.getScale();
      int iPrecision = column.getPrecision();
      int iLengthInUnits = column.getLengthInUnits();
      int iLength = column.getLength();
      DataType dt = Shunting.convertTypeFromAccess(column,
        iPrecision, iScale, iLength, iLengthInUnits);
      PredefinedType pt = dt.getPredefinedType();
      if (pt != null)
      {
        iScale = pt.getScale();
        iPrecision = pt.getPrecision();
        iLength = pt.getLength();
        if (iLength == PredefinedType.iUNDEFINED)
        {
          if ((pt.getType() == PreType.CHAR) || 
              (pt.getType() == PreType.NCHAR) ||
              (pt.getType() == PreType.BINARY))
            iLength = 1;
        }
        else if (pt.getMultiplier() != null)
          iLength = pt.getMultiplier().getValue()*iLength;
      }
      PropertyMap pm = column.getProperties();
      row.put(sJDBC_TABLE_CAT, sCatalog);
      row.put(sJDBC_TABLE_SCHEM, sSchema);
      row.put(sJDBC_TABLE_NAME, sTableName);
      row.put(sJDBC_COLUMN_NAME, sColumnName);
      int iSqlType = Types.NULL;
      if (dt.getType() != DataType.Type.ARRAY)
        iSqlType = dt.getPredefinedType().getType().getSqlType();
      else
        iSqlType = Types.ARRAY;
      row.put(sJDBC_DATA_TYPE, Integer.valueOf(iSqlType));
      String sTypeName = column.getType().toString();
      if (dt.getType() == DataType.Type.ARRAY)
        sTypeName = dt.format();
      row.put(sJDBC_TYPE_NAME, sTypeName);
      if (iPrecision > 0)
        row.put(sJDBC_COLUMN_SIZE, Integer.valueOf(iPrecision));
      else
        row.put(sJDBC_COLUMN_SIZE, Integer.valueOf(iLength));
      row.put(sJDBC_BUFFER_LENGTH, null);
      row.put(sJDBC_DECIMAL_DIGITS, Integer.valueOf(iScale));
      row.put(sJDBC_NUM_PREC_RADIX, Integer.valueOf(10));
      int iNullability = DatabaseMetaData.attributeNullableUnknown;
      if (pm != null)
      {
        Boolean bRequired = (Boolean)pm.getValue("Required");
        if (bRequired != null)
        {
          if (bRequired.booleanValue())
            iNullability = DatabaseMetaData.attributeNoNulls;
          else
            iNullability = DatabaseMetaData.attributeNullable;
        }
      }
      row.put(sJDBC_NULLABLE, Integer.valueOf(iNullability));
      String sDescription = null;
      if (pm != null)
        sDescription = (String)pm.getValue("Description");
      row.put(sJDBC_REMARKS, sDescription);
      String sDefaultValue = null;
      if (pm != null)
        sDefaultValue = (String)pm.getValue("DefaultValue");
      row.put(sJDBC_COLUMN_DEF, sDefaultValue);
      row.put(sJDBC_SQL_DATA_TYPE, null);
      row.put(sJDBC_SQL_DATETIME_SUB, null);
      row.put(sJDBC_CHAR_OCTET_LENGTH, Integer.valueOf(column.getLength()));
      row.put(sJDBC_ORDINAL_POSITION, Integer.valueOf(column.getColumnIndex()+1));
      String sIsNullable = null;
      switch(iNullability)
      {
        case DatabaseMetaData.attributeNullableUnknown: sIsNullable = ""; break;
        case DatabaseMetaData.attributeNullable: sIsNullable = "YES"; break;
        case DatabaseMetaData.attributeNoNulls: sIsNullable = "NO"; break;
      }
      row.put(sJDBC_IS_NULLABLE, sIsNullable);
      row.put(sJDBC_SCOPE_CATALOG, null);
      row.put(sJDBC_SCOPE_SCHEMA, null);
      row.put(sJDBC_SCOPE_TABLE, null);
      row.put(sJDBC_SOURCE_DATA_TYPE, null); // only for DISTINCT or user-generated REF
      String sAutoIncrement = "NO";
      if (column.isAutoNumber())
        sAutoIncrement = "YES";
      row.put(sJDBC_IS_AUTOINCREMENT, sAutoIncrement);
      row.put(sJDBC_IS_GENERATEDCOLUMN, "NO");
    }
    catch (IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
    return row;    
  } /* getColumnRow */
  
  /*------------------------------------------------------------------*/
  /** create and fill a row of JDBC column description from the
   * column descriptions of a view.
   * @param sCatalog catalog.
   * @param sSchema schema.
   * @param sTableName table name.
   * @param iColumnIndex column index (1-based).
   * @param sColumnName column name.
   * @param iDataType data type.
   * @param sTypeName type name.
   * @param iPrecision precision of type.
   * @param iScale scale of type.
   * @return row of JDBC column description.
   */
  private ResultSetRow getColumnRow(String sCatalog, String sSchema, 
    String sTableName, int iColumnIndex, String sColumnName, 
    int iDataType, String sTypeName, int iPrecision, int iScale) 
  {
    DataType dt = Shunting.convertTypeFromJdbc(iDataType, iPrecision, iScale);
    ResultSetRow row = new ResultSetRow();
    row.put(sJDBC_TABLE_CAT, sCatalog);
    row.put(sJDBC_TABLE_SCHEM, sSchema);
    row.put(sJDBC_TABLE_NAME, sTableName);
    row.put(sJDBC_COLUMN_NAME, sColumnName);
    row.put(sJDBC_DATA_TYPE, iDataType);
    row.put(sJDBC_TYPE_NAME, dt.format());
    row.put(sJDBC_COLUMN_SIZE, Integer.valueOf(iPrecision));
    row.put(sJDBC_BUFFER_LENGTH, Integer.valueOf(0)); 
    row.put(sJDBC_DECIMAL_DIGITS, Integer.valueOf(iScale));
    row.put(sJDBC_NUM_PREC_RADIX, Integer.valueOf(10));
    int iNullability = DatabaseMetaData.attributeNullableUnknown;
    row.put(sJDBC_NULLABLE, Integer.valueOf(iNullability));
    String sDescription = null;
    row.put(sJDBC_REMARKS, sDescription);
    String sDefaultValue = null;
    row.put(sJDBC_COLUMN_DEF, sDefaultValue);
    row.put(sJDBC_SQL_DATA_TYPE, null);
    row.put(sJDBC_SQL_DATETIME_SUB, null);
    Charset cs = _conn.getDatabase().getCharset();
    int iBytesPerChar = 1;
    if (cs.name().startsWith("UTF"))
      iBytesPerChar = 2;
    row.put(sJDBC_CHAR_OCTET_LENGTH, Integer.valueOf(iBytesPerChar));
    row.put(sJDBC_ORDINAL_POSITION, Integer.valueOf(iColumnIndex+1));
    String sIsNullable = null;
    switch(iNullability)
    {
      case DatabaseMetaData.attributeNullableUnknown: sIsNullable = ""; break;
      case DatabaseMetaData.attributeNullable: sIsNullable = "YES"; break;
      case DatabaseMetaData.attributeNoNulls: sIsNullable = "NO"; break;
    }
    row.put(sJDBC_IS_NULLABLE, sIsNullable);
    row.put(sJDBC_SCOPE_CATALOG, null);
    row.put(sJDBC_SCOPE_SCHEMA, null);
    row.put(sJDBC_SCOPE_TABLE, null);
    row.put(sJDBC_SOURCE_DATA_TYPE, null); // only for DISTINCT or user-generated REF
    String sAutoIncrement = "NO";
    row.put(sJDBC_IS_AUTOINCREMENT, sAutoIncrement);
    row.put(sJDBC_IS_GENERATEDCOLUMN, "NO");
    return row;    
  } /* getColumnRow */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getColumns(String sCatalog, String sSchemaPattern, String sTableNamePattern, String sColumnNamePattern) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sCOLUMNS);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_SIZE, java.sql.Types.INTEGER); // e.g. max chars in column
    rsh.addColumn(sJDBC_BUFFER_LENGTH, java.sql.Types.INTEGER); // not used
    rsh.addColumn(sJDBC_DECIMAL_DIGITS, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_NUM_PREC_RADIX, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_NULLABLE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_DEF, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SQL_DATA_TYPE, java.sql.Types.INTEGER); // unused
    rsh.addColumn(sJDBC_SQL_DATETIME_SUB, java.sql.Types.INTEGER); // unused
    rsh.addColumn(sJDBC_CHAR_OCTET_LENGTH, java.sql.Types.INTEGER); // max bytes in character
    rsh.addColumn(sJDBC_ORDINAL_POSITION, java.sql.Types.INTEGER); // 1-based
    rsh.addColumn(sJDBC_IS_NULLABLE, java.sql.Types.VARCHAR); // YES, NO or empty
    rsh.addColumn(sJDBC_SCOPE_CATALOG, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SCOPE_SCHEMA, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SCOPE_TABLE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SOURCE_DATA_TYPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_IS_AUTOINCREMENT, java.sql.Types.VARCHAR); // YES, NO or empty
    rsh.addColumn(sJDBC_IS_GENERATEDCOLUMN, java.sql.Types.VARCHAR); // YES, NO or empty
    List<Row> listColumns = new ArrayList<Row>();
    ResultSet rsTables = getTables(sCatalog, sSchemaPattern, sTableNamePattern, null);
    if (rsTables != null)
    {
      while(rsTables.next())
      {
        String sSchema = rsTables.getString(sJDBC_TABLE_SCHEM);
        String sTableName = rsTables.getString(sJDBC_TABLE_NAME);
        String sTableType = rsTables.getString(sJDBC_TABLE_TYPE);
        try
        {
          if (sTableType.equals(sJDBC_TABLE_TYPE_TABLE))
          {
            Table table = _conn.getDatabase().getTable(sTableName);
            List<? extends Column> listTableColumns = table.getColumns();
            for (int iColumn = 0; iColumn < listTableColumns.size(); iColumn++)
            {
              Column column = listTableColumns.get(iColumn);
              String sColumnName = column.getName();
              if (matches(sColumnNamePattern,sColumnName))
                listColumns.add(getColumnRow(sCatalog, sSchema, sTableName, sColumnName, column));
            }
          }
          else if (sTableType.equals(sJDBC_TABLE_TYPE_VIEW))
          {
            SelectQuery sq = _mapViews.get(sTableName);
            String sSql = getQuery(sq);
            Statement stmt = _conn.createStatement();
            AccessResultSet rsQuery = (AccessResultSet)stmt.executeQuery(sSql);
            ResultSetMetaData rsmd = rsQuery.getMetaData();
            List<String> listColumnNames = sq.getSelectColumns();
            for (int iColumn = 0; iColumn < listColumnNames.size(); iColumn++)
            {
              String sColumnName = listColumnNames.get(iColumn);
              int iDataType = rsmd.getColumnType(iColumn+1);
              int iPrecision = rsmd.getPrecision(iColumn+1);
              int iScale = rsmd.getScale(iColumn+1);
              String sTypeName = rsmd.getColumnTypeName(iColumn+1);
              if (matches(sColumnNamePattern,sColumnName))
                listColumns.add(getColumnRow(sCatalog, sSchema, sTableName, iColumn, sColumnName, iDataType, sTypeName, iPrecision, iScale));
            }
            rsQuery.close();
            stmt.close();
          }
        }
        catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
      }
    }
    Collections.sort(listColumns, new ColumnsComparator());
    MetaDataCursor mdc = new MetaDataCursor(listColumns);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getColumns */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getColumnPrivileges(String sCatalog, String sSchema, String sTable, String sColumnNamePattern) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sCOLUMN_PRIVILEGES);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_GRANTOR, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_GRANTEE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PRIVILEGE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_IS_GRANTABLE, java.sql.Types.VARCHAR);
    List<Row> listColumnPrivileges = new ArrayList<Row>();
    Collections.sort(listColumnPrivileges, new PrivilegesComparator());
    MetaDataCursor mdc = new MetaDataCursor(listColumnPrivileges);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getColumnPrivileges */
  
  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of primary keys */
  class PrimaryKeysComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      String sColumn1 = row1.getString(sJDBC_COLUMN_NAME);
      String sColumn2 = row2.getString(sJDBC_COLUMN_NAME);
      return compareStrings(sColumn1,sColumn2);
    } /* compare */
  } /* PrimaryKeysComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getPrimaryKeys(String sCatalog, String sSchema, String sTable)
      throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sPRIMARY_KEYS);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_KEY_SEQ, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_PK_NAME, java.sql.Types.VARCHAR);
    List<Row> listPrimaryKeys = new ArrayList<Row>();
    
    if ((sCatalog == null) || sCatalog.equals(_conn.getCatalog()))
    {
      String sSchemaName = getUserName();
      if ((sSchema == null) || (sSchema.equals(sSchemaName)))
      {
        try
        {
          Table table = _conn.getDatabase().getTable(sTable);
          if (table != null)
          {
            /* if a primary key exists, display its columns */
            Index indexPk = null;
            try { indexPk = table.getPrimaryKeyIndex(); }
            catch(IllegalArgumentException iae) {}
            if (indexPk != null)
            {
              List<? extends Index.Column> listPkColumns = indexPk.getColumns();
              for (int iPkColumn = 0; iPkColumn < listPkColumns.size(); iPkColumn++)
              {
                Index.Column pc = listPkColumns.get(iPkColumn);
                ResultSetRow row = new ResultSetRow();
                row.put(sJDBC_TABLE_CAT, sCatalog);
                row.put(sJDBC_TABLE_SCHEM, sSchema);
                row.put(sJDBC_TABLE_NAME, sTable);
                row.put(sJDBC_COLUMN_NAME, pc.getName());
                row.put(sJDBC_KEY_SEQ, Short.valueOf((short)(iPkColumn+1)));
                row.put(sJDBC_PK_NAME, indexPk.getName());
                listPrimaryKeys.add(row);
              }
            }
          }
        }
        catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
      }
    }    
    Collections.sort(listPrimaryKeys, new PrimaryKeysComparator());
    MetaDataCursor mdc = new MetaDataCursor(listPrimaryKeys);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getPrimaryKeys */

  /*------------------------------------------------------------------*/
  /** 
   * Returns true, if the two indexes refer to the same columns.
   * @param index1 first index.
   * @param index2 second index.
   * @return true, if the two indexes refer to the same columns.
   */
  private boolean matchIndexColumns(Index index1, Index index2)
  {
    boolean bMatch = true;
    if (index1 != index2)
    {
      List<? extends Index.Column> listIndex1Columns = index1.getColumns();
      List<? extends Index.Column> listIndex2Columns = index2.getColumns();
      if (listIndex1Columns.size() == listIndex2Columns.size())
      {
        for (int iColumn = 0; bMatch && (iColumn < listIndex1Columns.size()); iColumn++)
        {
          Index.Column ic1 = listIndex1Columns.get(iColumn);
          Index.Column ic2 = listIndex2Columns.get(iColumn);
          if (!ic1.getName().equals(ic2.getName()))
            bMatch = false;
        }
      }
      else
        bMatch = false;
    }
    return bMatch;
  } /* matchIndexColumns */
  
  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of exported keys */
  class ForeignKeyComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareColumns(
          row1.getString(sJDBC_FKTABLE_CAT),
          row1.getString(sJDBC_FKTABLE_SCHEM),
          row1.getString(sJDBC_FKTABLE_NAME),
          Integer.valueOf((row1.getShort(sJDBC_KEY_SEQ)).intValue()),
          row2.getString(sJDBC_FKTABLE_CAT),
          row2.getString(sJDBC_FKTABLE_SCHEM),
          row2.getString(sJDBC_FKTABLE_NAME),
          Integer.valueOf((row2.getShort(sJDBC_KEY_SEQ)).intValue()));
      return iCompare;
    } /* compare */
  } /* ForeignKeyComparator */
  
  /*------------------------------------------------------------------*/
  /** Create a header for exported-key, imported-key, cross-reference 
   * result sets.
   * @param sResultSetName name for result set.
   * @return header for foreign key result sets.
   */
  private ResultSetHeader getForeignKeyHeader(String sResultSetName)
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sResultSetName);
    rsh.addColumn(sJDBC_PKTABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PKTABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PKTABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PKCOLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FKTABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FKTABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FKTABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FKCOLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_KEY_SEQ, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_UPDATE_RULE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_DELETE_RULE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FK_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PK_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DEFERRABILITY, java.sql.Types.VARCHAR);
    return rsh;
  } /* getForeignKeyHeader */

  /*------------------------------------------------------------------*/
  /** Create a row for exported-key, imported-key, cross-reference 
   * result sets.
   * @param sFkCatalog catalog.
   * @param sFkSchema schema.
   * @param sPkCatalog catalog.
   * @param sPkSchema schema.
   * @param indexFk foreign key index.
   * @param listFkColumns list of columns of foreign key index.
   * @param indexRef referenced (primary key) index. 
   * @param listRefColumns list of columns of referenced (primary key) index.
   * @param iColumn column index in the two lists.
   * @return row with data describing the foreign key column.
   */
  private ResultSetRow getForeignKeyRow(
      String sFkCatalog, String sFkSchema,
      String sPkCatalog, String sPkSchema,
    Index indexFk, List<? extends Index.Column> listFkColumns,
    Index indexRef, List<? extends Index.Column> listRefColumns,
    int iColumn)
  {
    ResultSetRow row = new ResultSetRow();
    Index.Column icRef = listRefColumns.get(iColumn);
    Index.Column icFk = listFkColumns.get(iColumn);
    row.put(sJDBC_PKTABLE_CAT, sPkCatalog);
    row.put(sJDBC_PKTABLE_SCHEM, sPkSchema);
    row.put(sJDBC_PKTABLE_NAME, indexRef.getTable().getName());
    row.put(sJDBC_PKCOLUMN_NAME, icRef.getName());
    row.put(sJDBC_FKTABLE_CAT, sFkCatalog);
    row.put(sJDBC_FKTABLE_SCHEM, sFkSchema);
    row.put(sJDBC_FKTABLE_NAME, indexFk.getTable().getName());
    row.put(sJDBC_FKCOLUMN_NAME, icFk.getName());
    row.put(sJDBC_KEY_SEQ, Short.valueOf((short)(iColumn+1)));
    row.put(sJDBC_UPDATE_RULE, Short.valueOf((short)DatabaseMetaData.importedKeyNoAction));
    row.put(sJDBC_DELETE_RULE, Short.valueOf((short)DatabaseMetaData.importedKeyNoAction));
    row.put(sJDBC_FK_NAME, indexFk.getName());
    row.put(sJDBC_PK_NAME, indexRef.getName());
    row.put(sJDBC_DEFERRABILITY, Short.valueOf((short)DatabaseMetaData.importedKeyNotDeferrable));
    return row;    
  } /* getForeignKeyRow */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getExportedKeys(String sCatalog, String sSchema, String sTable)
      throws SQLException
  {
    ResultSetHeader rsh = getForeignKeyHeader(sEXPORTED_KEYS);
    List<Row> listExportedKeys = new ArrayList<Row>();
    
    if ((sCatalog == null) || sCatalog.equals(_conn.getCatalog()))
    {
      String sSchemaName = getUserName();
      if ((sSchema == null) || (sSchema.equals(sSchemaName)))
      {
        try
        {
          Table table = _conn.getDatabase().getTable(sTable);
          if (table != null)
          {
            /* if a primary key exists, find the foreign keys pointing at it */
            Index indexPk = null;
            try { indexPk = table.getPrimaryKeyIndex(); }
            catch(IllegalArgumentException iae) {}
            if (indexPk != null)
            {
              /* find all other tables with foreign keys connecting to this */
              for (Iterator<String> iterTableName = _conn.getDatabase().getTableNames().iterator(); iterTableName.hasNext(); )
              {
                String sTableName = iterTableName.next();
                Table tableFk = _conn.getDatabase().getTable(sTableName);
                Index indexFk = null;
                try { indexFk = tableFk.getForeignKeyIndex(table); }
                catch (IllegalArgumentException iae) {}
                if (indexFk != null)
                {
                  /* there is a foreign-key relationship in either direction */
                  Index indexRef = indexFk.getReferencedIndex();
                  if (indexRef.getTable().getName().equals(sTable))
                  {
                    /* are the referenced columns primary key columns? */
                    if (indexRef.isPrimaryKey() || matchIndexColumns(indexRef,indexPk))
                    {
                      if (indexFk.getName().startsWith("."))
                        indexFk = indexRef;
                      List<? extends Index.Column> listFkColumns = indexFk.getColumns();
                      List<? extends Index.Column> listPkColumns = indexPk.getColumns();
                      for (int iColumn = 0; iColumn < listPkColumns.size(); iColumn++)
                      {
                        ResultSetRow row = getForeignKeyRow(
                            sCatalog, sSchemaName,
                            sCatalog, sSchemaName,
                            indexFk, listFkColumns,
                            indexPk, listPkColumns,
                            iColumn);
                        listExportedKeys.add(row);
                      }
                    }
                  }
                }
              }
            }
          }
        }
        catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
      }
    }
    /* sort exported keys by FK */
    Collections.sort(listExportedKeys, new ForeignKeyComparator());
    MetaDataCursor mdc = new MetaDataCursor(listExportedKeys);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getExportedKeys */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of imported keys */
  class PrimaryKeyComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareColumns(
          row1.getString(sJDBC_PKTABLE_CAT),
          row1.getString(sJDBC_PKTABLE_SCHEM),
          row1.getString(sJDBC_PKTABLE_NAME),
          Integer.valueOf((row1.getShort(sJDBC_KEY_SEQ)).intValue()),
          row2.getString(sJDBC_PKTABLE_CAT),
          row2.getString(sJDBC_PKTABLE_SCHEM),
          row2.getString(sJDBC_PKTABLE_NAME),
          Integer.valueOf((row2.getShort(sJDBC_KEY_SEQ)).intValue()));
      return iCompare;
    } /* compare */
  } /* PrimaryKeyComparator */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getImportedKeys(String sCatalog, String sSchema, String sTable)
      throws SQLException
  {
    ResultSetHeader rsh = getForeignKeyHeader(sIMPORTED_KEYS);
    List<Row> listImportedKeys = new ArrayList<Row>();
    if ((sCatalog == null) || sCatalog.equals(_conn.getCatalog()))
    {
      String sSchemaName = getUserName();
      if ((sSchema == null) || (sSchema.equals(sSchemaName)))
      {
        try
        {
          Table table = _conn.getDatabase().getTable(sTable);
          if (table != null)
          {
            /* find all foreign keys referencing primary keys of another table */
            List<? extends Index> listFkIndexes = table.getIndexes();
            for (Iterator<? extends Index> iterFkIndex = listFkIndexes.iterator(); iterFkIndex.hasNext(); )
            {
              Index indexFk = iterFkIndex.next();
              if (indexFk.isForeignKey())
              {
                Index indexRef = indexFk.getReferencedIndex();
                try
                {
                  /* are the referenced columns primary key columns? */
                  Index indexPk = indexRef.getTable().getPrimaryKeyIndex();
                  if ((indexPk != null) && 
                      (indexRef.isPrimaryKey() || matchIndexColumns(indexRef,indexPk)))
                  {
                    if (indexFk.getName().startsWith("."))
                      indexFk = indexRef;
                    List<? extends Index.Column> listFkColumns = indexFk.getColumns();
                    List<? extends Index.Column> listPkColumns = indexPk.getColumns();
                    for (int iColumn = 0; iColumn < listPkColumns.size(); iColumn++)
                    {
                      ResultSetRow row = getForeignKeyRow(
                          sCatalog, sSchemaName,
                          sCatalog, sSchemaName,
                          indexFk, listFkColumns,
                          indexPk, listPkColumns,
                          iColumn);
                      listImportedKeys.add(row);
                    }
                  }
                }
                catch(IllegalArgumentException iae) {}
              }
            }
          }
        }
        catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
      }
    }
    /* sort imported keys by PK */
    Collections.sort(listImportedKeys, new PrimaryKeyComparator());
    MetaDataCursor mdc = new MetaDataCursor(listImportedKeys);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getImportedKeys */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getCrossReference(
    String sPkCatalog, String sPkSchema, String sPkTable,
    String sFkCatalog, String sFkSchema, String sFkTable)
    throws SQLException
  {
    ResultSetHeader rsh = getForeignKeyHeader(sCROSS_REFERENCE);
    /* this time it is about any foreign key from fktable to pktable */
    List<Row> listCrossReference = new ArrayList<Row>();
    if (((sPkCatalog == null) || sPkCatalog.equals(_conn.getCatalog())) &&
        ((sFkCatalog == null) || sFkCatalog.equals(_conn.getCatalog())))
    {
      String sSchemaName = getUserName();
      if (((sPkSchema == null) || (sPkSchema.equals(sSchemaName))) &&
          ((sFkSchema == null) || (sFkSchema.equals(sSchemaName))))
      {
        try
        {
          Table tableFk = _conn.getDatabase().getTable(sFkTable);
          Table tablePk = _conn.getDatabase().getTable(sPkTable);
          if ((tableFk != null) && (tablePk != null))
          {
            List<? extends Index> listFkIndexes = tableFk.getIndexes();
            for (Iterator<? extends Index> iterFkIndex = listFkIndexes.iterator(); iterFkIndex.hasNext(); )
            {
              Index indexFk = iterFkIndex.next();
              if (indexFk.isForeignKey())
              {
                Index indexRef = indexFk.getReferencedIndex();
                if (indexRef.getTable().getName().equals(sPkTable))
                {
                  List<? extends Index.Column> listFkColumns = indexFk.getColumns();
                  List<? extends Index.Column> listRefColumns = indexRef.getColumns();
                  for (int iColumn = 0; iColumn < listRefColumns.size(); iColumn++)
                  {
                    ResultSetRow row = getForeignKeyRow(
                        sFkCatalog, sFkSchema,
                        sPkCatalog, sPkSchema,
                        indexFk, listFkColumns,
                        indexRef, listRefColumns,
                        iColumn);
                    listCrossReference.add(row);
                  }
                }
              }
            }
          }
        }
        catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
      }
    }
    /* sort exported keys by FK */
    Collections.sort(listCrossReference, new ForeignKeyComparator());
    MetaDataCursor mdc = new MetaDataCursor(listCrossReference);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getCrossReference */

  /*------------------------------------------------------------------*/
  /** Compares two Integer values
   * @param i1 first Integer value.
   * @param i2 second Integer value.
   * @return -1 if first i1 less than i2, 0 if they are equal, 1 otherwise. 
   */
  int compareIntegers(Integer i1, Integer i2)
  {
    int iCompare = 0;
    if ((i1 != null) && (i2 != null))
      iCompare = i1.compareTo(i2);
    else if ((i1 == null) && (i2 != null))
      iCompare = -1;
    else if ((i1 != null) && (i2 == null))
      iCompare = 1;
    return iCompare;
  } /* compareIntegers */
  
  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of type info */
  class TypeInfoComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      Integer iDataType1 = row1.getInt(sJDBC_DATA_TYPE);
      Integer iDataType2 = row2.getInt(sJDBC_DATA_TYPE);
      int iCompare = compareIntegers(
          iDataType1,
          iDataType2);
      if (iCompare == 0)
      {
        String sTypeName1 = row1.getString(sJDBC_TYPE_NAME);
        String sTypeName2 = row2.getString(sJDBC_TYPE_NAME);
        com.healthmarketscience.jackcess.DataType dt1 = com.healthmarketscience.jackcess.DataType.valueOf(sTypeName1);
        com.healthmarketscience.jackcess.DataType dt2 = com.healthmarketscience.jackcess.DataType.valueOf(sTypeName2);
        boolean bMatch1 = false;
        boolean bMatch2 = false;
        try
        {
          if (iDataType1 != null)
            bMatch1 = iDataType1.intValue() == dt1.getSQLType();
          if (iDataType1 != null)
            bMatch2 = iDataType2.intValue() == dt2.getSQLType();
        }
        catch (SQLException se) {}
        if (bMatch1 && (!bMatch2))
          iCompare = -1;
        else if ((!bMatch1) && bMatch2)
          iCompare = 1;
        else
          iCompare = sTypeName1.compareTo(sTypeName2);
      }
      return iCompare;
    } /* compare */
  } /* TypeInfoComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getTypeInfo() throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sTYPE_INFO);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_PRECISION, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_LITERAL_PREFIX, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_LITERAL_SUFFIX, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_NULLABLE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_CASE_SENSITIVE, java.sql.Types.BOOLEAN);
    rsh.addColumn(sJDBC_SEARCHABLE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_UNSIGNED_ATTRIBUTE, java.sql.Types.BOOLEAN);
    rsh.addColumn(sJDBC_FIXED_PREC_SCALE, java.sql.Types.BOOLEAN);
    rsh.addColumn(sJDBC_AUTOINCREMENT, java.sql.Types.BOOLEAN);
    rsh.addColumn(sJDBC_LOCAL_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_MINIMUM_SCALE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_MAXIMUM_SCALE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_SQL_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_SQL_DATETIME_SUB, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_NUM_PREC_RADIX, java.sql.Types.INTEGER);
    List<Row> listTypeInfo = new ArrayList<Row>();
    /* loop over the DataType enum */
    for (int iDataType = 0; iDataType < com.healthmarketscience.jackcess.DataType.values().length; iDataType++)
    {
      com.healthmarketscience.jackcess.DataType dt = com.healthmarketscience.jackcess.DataType.values()[iDataType];
      Integer iSqlType = null;
      try { iSqlType = Integer.valueOf(dt.getSQLType()); }
      catch (SQLException se)
      { 
        // System.err.println(se.getClass().getName()+": "+se.getMessage());
        if (dt == com.healthmarketscience.jackcess.DataType.GUID)
          iSqlType = Integer.valueOf(java.sql.Types.VARCHAR);
        else if (dt == com.healthmarketscience.jackcess.DataType.UNSUPPORTED_FIXEDLEN)
          iSqlType = Integer.valueOf(java.sql.Types.BINARY);
        else if (dt == com.healthmarketscience.jackcess.DataType.UNSUPPORTED_VARLEN)
          iSqlType = Integer.valueOf(java.sql.Types.BINARY);
        else
          iSqlType = Integer.valueOf(java.sql.Types.OTHER);
      }
      ResultSetRow row = new ResultSetRow();
      row.put(sJDBC_TYPE_NAME, dt.name());
      row.put(sJDBC_DATA_TYPE, iSqlType);
      row.put(sJDBC_PRECISION, Integer.valueOf(dt.getDefaultPrecision()));
      String sLiteralPrefix = null;
      String sLiteralSuffix = null;
      if ((dt == com.healthmarketscience.jackcess.DataType.MEMO) ||
          (dt == com.healthmarketscience.jackcess.DataType.GUID) ||
          (dt == com.healthmarketscience.jackcess.DataType.TEXT) ||
          (dt == com.healthmarketscience.jackcess.DataType.SHORT_DATE_TIME) ||
          (dt == com.healthmarketscience.jackcess.DataType.BINARY))
      {
        sLiteralPrefix = "'";
        sLiteralSuffix = "'";
      }
      row.put(sJDBC_LITERAL_PREFIX, sLiteralPrefix);
      row.put(sJDBC_LITERAL_SUFFIX, sLiteralSuffix);
      row.put(sJDBC_NULLABLE, Integer.valueOf(DatabaseMetaData.typeNullable));
      row.put(sJDBC_CASE_SENSITIVE, Boolean.valueOf(dt.isTextual()));
      row.put(sJDBC_SEARCHABLE, Integer.valueOf(DatabaseMetaData.typeSearchable));
      row.put(sJDBC_UNSIGNED_ATTRIBUTE, Boolean.valueOf(false));
      row.put(sJDBC_FIXED_PREC_SCALE, Boolean.valueOf(dt == com.healthmarketscience.jackcess.DataType.MONEY));
      row.put(sJDBC_AUTOINCREMENT, Boolean.valueOf(false));
      row.put(sJDBC_LOCAL_TYPE_NAME, null);
      row.put(sJDBC_MINIMUM_SCALE, Integer.valueOf(dt.getMinScale()));
      row.put(sJDBC_MAXIMUM_SCALE, Integer.valueOf(dt.getMaxScale()));
      row.put(sJDBC_SQL_DATA_TYPE, null);
      row.put(sJDBC_SQL_DATETIME_SUB, null);
      row.put(sJDBC_NUM_PREC_RADIX, Integer.valueOf(10));
      listTypeInfo.add(row);
    }
    /* sort by DATA_TYPE and "closeness" of match */
    Collections.sort(listTypeInfo, new TypeInfoComparator());
    MetaDataCursor mdc = new MetaDataCursor(listTypeInfo);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getTypeInfo */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of index info */
  class IndexInfoComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = 0;
      if (iCompare == 0)
      {
        Boolean bNonUnique1 = row1.getBoolean(sJDBC_NON_UNIQUE);
        Boolean bNonUnique2 = row2.getBoolean(sJDBC_NON_UNIQUE);
        iCompare = bNonUnique1.compareTo(bNonUnique2);
      }
      if (iCompare == 0)
      {
        Short wType1 = row1.getShort(sJDBC_TYPE);
        Short wType2 = row2.getShort(sJDBC_TYPE);
        iCompare = wType1.compareTo(wType2);
      }
      if (iCompare == 0)
      {
        String sIndexName1 = row1.getString(sJDBC_INDEX_NAME);
        String sIndexName2 = row2.getString(sJDBC_INDEX_NAME);
        iCompare = sIndexName1.compareTo(sIndexName2);
      }
      if (iCompare == 0)
      {
        Short wOrdinalPosition1 = row1.getShort(sJDBC_ORDINAL_POSITION);
        Short wOrdinalPosition2 = row2.getShort(sJDBC_ORDINAL_POSITION);
        iCompare = wOrdinalPosition1.compareTo(wOrdinalPosition2);
      }
      return iCompare;
    } /* compare */
  } /* IndexInfoComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} */
  @Override
  public ResultSet getIndexInfo(String sCatalog, String sSchema, String sTable,
      boolean bUnique, boolean bApproximate) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sINDEX_INFO);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_NON_UNIQUE, java.sql.Types.BOOLEAN);
    rsh.addColumn(sJDBC_INDEX_QUALIFIER, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_INDEX_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_ORDINAL_POSITION, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_ASC_OR_DESC, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_CARDINALITY, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_PAGES, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_FILTER_CONDITION, java.sql.Types.VARCHAR);
    List<Row> listIndexInfo = new ArrayList<Row>();
    if ((sCatalog == null) || sCatalog.equals(_conn.getCatalog()))
    {
      String sSchemaName = getUserName();
      if ((sSchema == null) || (sSchema.equals(sSchemaName)))
      {
        try
        {
          Table table = _conn.getDatabase().getTable(sTable);
          if (table != null)
          {
            List<? extends Index> listIndexes = table.getIndexes();
            for (int iIndex = 0; iIndex < listIndexes.size(); iIndex++)
            {
              Index index = listIndexes.get(iIndex);
              /* indexes starting with "." are "hidden" reference indexes */
              if (((!bUnique) || (index.isUnique())) && (!index.getName().startsWith(".")))  
              {
                List<? extends Index.Column> listIndexColumns = index.getColumns();
                for (int iColumn = 0; iColumn < listIndexColumns.size(); iColumn++)
                {
                  Index.Column ic = listIndexColumns.get(iColumn);
                  Column column = ic.getColumn();
                  // ignore indexes on complex columns
                  Short wIndexType = DatabaseMetaData.tableIndexHashed;
                  if (column.getType() == com.healthmarketscience.jackcess.DataType.COMPLEX_TYPE)
                    wIndexType = DatabaseMetaData.tableIndexOther;
                  ResultSetRow row = new ResultSetRow();
                  row.put(sJDBC_TABLE_CAT, sCatalog);
                  row.put(sJDBC_TABLE_SCHEM, sSchema);
                  row.put(sJDBC_TABLE_NAME, table.getName());
                  row.put(sJDBC_NON_UNIQUE, Boolean.valueOf(!index.isUnique()));
                  row.put(sJDBC_INDEX_QUALIFIER, null);
                  row.put(sJDBC_INDEX_NAME, index.getName());
                  row.put(sJDBC_TYPE, wIndexType);
                  row.put(sJDBC_ORDINAL_POSITION, Short.valueOf((short)(iColumn+1)));
                  row.put(sJDBC_COLUMN_NAME, ic.getName());
                  row.put(sJDBC_ASC_OR_DESC, null);
                  row.put(sJDBC_CARDINALITY, Integer.valueOf(table.getRowCount())); 
                  row.put(sJDBC_PAGES, Integer.valueOf(-1));
                  row.put(sJDBC_FILTER_CONDITION, null);
                  listIndexInfo.add(row);
                }
              }
            }
          }
        }
        catch(IOException ie) { throw new SQLException(ie.getClass().getName()+": "+ie.getMessage()); }
      }
    }
    Collections.sort(listIndexInfo, new IndexInfoComparator());
    MetaDataCursor mdc = new MetaDataCursor(listIndexInfo);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getIndexInfo */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of functions */
  class FunctionsComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
          row1.getString(sJDBC_FUNCTION_CAT),
          row1.getString(sJDBC_FUNCTION_SCHEM),
          row1.getString(sJDBC_FUNCTION_NAME),
          row1.getString(sJDBC_SPECIFIC_NAME),
          row2.getString(sJDBC_FUNCTION_CAT),
          row2.getString(sJDBC_FUNCTION_SCHEM),
          row2.getString(sJDBC_FUNCTION_NAME),
          row2.getString(sJDBC_SPECIFIC_NAME)
          );
      return iCompare;
    } /* compare */
  } /* FunctionsComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty result set */
  @Override
  public ResultSet getFunctions(String sCatalog, String sSchemaPattern, String sFunctionNamePattern)
      throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sFUNCTIONS);
    rsh.addColumn(sJDBC_FUNCTION_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FUNCTION_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FUNCTION_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FUNCTION_TYPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_SPECIFIC_NAME, java.sql.Types.VARCHAR);
    List<Row> listFunctions = new ArrayList<Row>();
    Collections.sort(listFunctions, new FunctionsComparator());
    MetaDataCursor mdc = new MetaDataCursor(listFunctions);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getFunctions */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of function columns */
  class FunctionColumnsComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
          row1.getString(sJDBC_FUNCTION_CAT),
          row1.getString(sJDBC_FUNCTION_SCHEM),
          row1.getString(sJDBC_FUNCTION_NAME),
          row1.getString(sJDBC_SPECIFIC_NAME),
          row2.getString(sJDBC_FUNCTION_CAT),
          row2.getString(sJDBC_FUNCTION_SCHEM),
          row2.getString(sJDBC_FUNCTION_NAME),
          row2.getString(sJDBC_SPECIFIC_NAME)
          );
      if (iCompare == 0)
      {
        Integer iOrdinalPosition1 = row1.getInt(sJDBC_ORDINAL_POSITION);
        Integer iOrdinalPosition2 = row2.getInt(sJDBC_ORDINAL_POSITION);
        iCompare = iOrdinalPosition1.compareTo(iOrdinalPosition2);
      }
      return iCompare;
    } /* compare */
  } /* FunctionColumnsComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty result set */
  @Override
  public ResultSet getFunctionColumns(String sCatalog, String sSchemaPattern,
      String sFunctionNamePattern, String sColumnNamePattern) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sFUNCTION_COLUMNS);
    rsh.addColumn(sJDBC_FUNCTION_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FUNCTION_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_FUNCTION_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_TYPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PRECISION, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_LENGTH, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_SCALE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_RADIX, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_NULLABLE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_CHAR_OCTET_LENGTH, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_ORDINAL_POSITION, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_IS_NULLABLE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SPECIFIC_NAME, java.sql.Types.VARCHAR);
    List<Row> listFunctionColumns = new ArrayList<Row>();
    Collections.sort(listFunctionColumns, new FunctionColumnsComparator());
    MetaDataCursor mdc = new MetaDataCursor(listFunctionColumns);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getFunctionColumns */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of procedures */
  class ProceduresComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
          row1.getString(sJDBC_PROCEDURE_CAT),
          row1.getString(sJDBC_PROCEDURE_SCHEM),
          row1.getString(sJDBC_PROCEDURE_NAME),
          row1.getString(sJDBC_SPECIFIC_NAME),
          row2.getString(sJDBC_PROCEDURE_CAT),
          row2.getString(sJDBC_PROCEDURE_SCHEM),
          row2.getString(sJDBC_PROCEDURE_NAME),
          row2.getString(sJDBC_PROCEDURE_NAME)
          );
      return iCompare;
    } /* compare */
  } /* ProceduresComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty result set */
  @Override
  public ResultSet getProcedures(String sCatalog, String sSchemaPattern, String sProcedureNamePattern)
      throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sPROCEDURES);
    rsh.addColumn(sJDBC_PROCEDURE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PROCEDURE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PROCEDURE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_RESERVED1, java.sql.Types.OTHER);
    rsh.addColumn(sJDBC_RESERVED2, java.sql.Types.OTHER);
    rsh.addColumn(sJDBC_RESERVED3, java.sql.Types.OTHER);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PROCEDURE_TYPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_SPECIFIC_NAME, java.sql.Types.VARCHAR);
    List<Row> listProcedures = new ArrayList<Row>();
    Collections.sort(listProcedures, new ProceduresComparator());
    MetaDataCursor mdc = new MetaDataCursor(listProcedures);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getProcedures */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of procedure columns */
  class ProcedureColumnsComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
          row1.getString(sJDBC_PROCEDURE_CAT),
          row1.getString(sJDBC_PROCEDURE_SCHEM),
          row1.getString(sJDBC_PROCEDURE_NAME),
          row1.getString(sJDBC_SPECIFIC_NAME),
          row2.getString(sJDBC_PROCEDURE_CAT),
          row2.getString(sJDBC_PROCEDURE_SCHEM),
          row2.getString(sJDBC_PROCEDURE_NAME),
          row2.getString(sJDBC_SPECIFIC_NAME)
          );
      if (iCompare == 0)
      {
        Integer iOrdinalPosition1 = row1.getInt(sJDBC_ORDINAL_POSITION);
        Integer iOrdinalPosition2 = row2.getInt(sJDBC_ORDINAL_POSITION);
        iCompare = iOrdinalPosition1.compareTo(iOrdinalPosition2);
      }
      return iCompare;
    } /* compare */
  } /* ProcedureColumnsComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty result set */
  @Override
  public ResultSet getProcedureColumns(String sCatalog, String sSchemaPattern,
      String ProcedureNamePattern, String sColumnNamePattern) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sPROCEDURE_COLUMNS);
    rsh.addColumn(sJDBC_PROCEDURE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PROCEDURE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PROCEDURE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_TYPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_PRECISION, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_LENGTH, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_SCALE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_RADIX, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_NULLABLE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_DEF, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SQL_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_SQL_DATETIME_SUB, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_CHAR_OCTET_LENGTH, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_ORDINAL_POSITION, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_IS_NULLABLE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SPECIFIC_NAME, java.sql.Types.VARCHAR);
    List<Row> listProcedureColumns = new ArrayList<Row>();
    Collections.sort(listProcedureColumns, new ProcedureColumnsComparator());
    MetaDataCursor mdc = new MetaDataCursor(listProcedureColumns);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getProcedureColumns */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getSuperTables(String arg0, String arg1, String arg2)
      throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sSUPER_TABLES);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SUPERTABLE_NAME, java.sql.Types.VARCHAR);
    List<Row> listSuperTables = new ArrayList<Row>();
    MetaDataCursor mdc = new MetaDataCursor(listSuperTables);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getSuperTables */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of best row identifier columns */
  class BestRowIdentifierComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = row1.getShort(sJDBC_SCOPE).compareTo(row2.getShort(sJDBC_SCOPE));
      return iCompare;
    } /* compare */
  } /* BestRowIdentifierComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getBestRowIdentifier(String sCatalog, String sSchema, String sTable, int iScope, boolean bNullable) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sBEST_ROW_IDENTIFIER);
    rsh.addColumn(sJDBC_SCOPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_SIZE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_BUFFER_LENGTH, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_DECIMAL_DIGITS, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_PSEUDO_COLUMN, java.sql.Types.SMALLINT);
    List<Row> listBestRowIdentifier = new ArrayList<Row>();
    Collections.sort(listBestRowIdentifier, new BestRowIdentifierComparator());
    MetaDataCursor mdc = new MetaDataCursor(listBestRowIdentifier);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getBestRowIdentifier */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of attributes columns */
  class AttributesComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareColumns(
          row1.getString(sJDBC_TYPE_CAT),
          row1.getString(sJDBC_TYPE_SCHEM),
          row1.getString(sJDBC_TYPE_NAME),
          row1.getInt(sJDBC_ORDINAL_POSITION),
          row2.getString(sJDBC_TYPE_CAT),
          row2.getString(sJDBC_TYPE_SCHEM),
          row2.getString(sJDBC_TYPE_NAME),
          row2.getInt(sJDBC_ORDINAL_POSITION));
      return iCompare;
    } /* compare */
  } /* AttributesComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getAttributes(String sCatalog, String sSchemaPattern, String sTypeNamePattern, String sAttributeNamePattern) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA, sATTRIBUTES);
    rsh.addColumn(sJDBC_TYPE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_ATTR_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_ATTR_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_ATTR_SIZE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_DECIMAL_DIGITS, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_NUM_PREC_RADIX, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_NULLABLE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_ATTR_DEF, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SQL_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_SQL_DATETIME_SUB, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_CHAR_OCTET_LENGTH, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_ORDINAL_POSITION, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_IS_NULLABLE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SCOPE_CATALOG, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SCOPE_SCHEMA, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SCOPE_TABLE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SOURCE_DATA_TYPE, java.sql.Types.SMALLINT);
    List<Row> listAttributes = new ArrayList<Row>();
    Collections.sort(listAttributes, new AttributesComparator());
    MetaDataCursor mdc = new MetaDataCursor(listAttributes);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getAttributes */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getSuperTypes(String sCatalog, String sSchemaPattern, String sTypeNamePattern)
      throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sSUPER_TYPES);
    rsh.addColumn(sJDBC_TYPE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SUPERTYPE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SUPERTYPE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_SUPERTYPE_NAME, java.sql.Types.VARCHAR);
    List<Row> listSuperTypes = new ArrayList<Row>();
    MetaDataCursor mdc = new MetaDataCursor(listSuperTypes);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getSuperTypes */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of UDTs columns */
  class UdtsComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = 0;
      Integer iDataType1 = row1.getInt(sJDBC_DATA_TYPE);
      Integer iDataType2 = row2.getInt(sJDBC_DATA_TYPE);
      iCompare = iDataType1.compareTo(iDataType2);
      if (iCompare == 0)
      {
        iCompare = compareStrings(
            row1.getString(sJDBC_TYPE_CAT),
            row1.getString(sJDBC_TYPE_SCHEM),
            row1.getString(sJDBC_TYPE_NAME),
            row2.getString(sJDBC_TYPE_CAT),
            row2.getString(sJDBC_TYPE_SCHEM),
            row2.getString(sJDBC_TYPE_NAME));
      }
      return iCompare;
    } /* compare */
  } /* UdtsComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getUDTs(String sCatalog, String sSchemaPattern, String sTypeNamePattern,
      int[] aiTypes) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sUDTS);
    rsh.addColumn(sJDBC_TYPE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_CLASS_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_BASE_TYPE, java.sql.Types.SMALLINT);
    List<Row> listUdts = new ArrayList<Row>();
    Collections.sort(listUdts,new UdtsComparator());
    MetaDataCursor mdc = new MetaDataCursor(listUdts);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getUDTs */

  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData}
   * returns empty set */
  @Override
  public ResultSet getVersionColumns(String sCatalog, String sSchema, String sTable) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sVERSION_COLUMNS);
    rsh.addColumn(sJDBC_SCOPE, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_TYPE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_SIZE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_BUFFER_LENGTH, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_DECIMAL_DIGITS, java.sql.Types.SMALLINT);
    rsh.addColumn(sJDBC_PSEUDO_COLUMN, java.sql.Types.SMALLINT);
    List<Row> listVersionColumns = new ArrayList<Row>();
    MetaDataCursor mdc = new MetaDataCursor(listVersionColumns);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getVersionColumns */

  /*------------------------------------------------------------------*/
  /** Comparator for sorting rows of pseudo columns */
  class PseudoColumnsComparator implements Comparator<Row> 
  {
    /** {@link Comparator} */
    @Override
    public int compare(Row row1, Row row2)
    {
      int iCompare = compareStrings(
          row1.getString(sJDBC_TABLE_CAT),
          row1.getString(sJDBC_TABLE_SCHEM),
          row1.getString(sJDBC_TABLE_NAME),
          row1.getString(sJDBC_COLUMN_NAME),
          row2.getString(sJDBC_TABLE_CAT),
          row2.getString(sJDBC_TABLE_SCHEM),
          row2.getString(sJDBC_TABLE_NAME),
          row2.getString(sJDBC_COLUMN_NAME));
      return iCompare;
    } /* compare */
  } /* PseudoColumnsComparator */
  
  /*------------------------------------------------------------------*/
  /** {@link DatabaseMetaData} for JDK 1.7 
   * returns empty set */
  @Override
  public ResultSet getPseudoColumns(String catalog,
      String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException
  {
    ResultSetHeader rsh = new ResultSetHeader(sINFORMATION_SCHEMA,sPSEUDO_COLUMNS);
    rsh.addColumn(sJDBC_TABLE_CAT, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_SCHEM, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_TABLE_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_COLUMN_NAME, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_DATA_TYPE, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_COLUMN_SIZE, java.sql.Types.INTEGER); // e.g. max chars in column
    rsh.addColumn(sJDBC_DECIMAL_DIGITS, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_NUM_PREC_RADIX, java.sql.Types.INTEGER);
    rsh.addColumn(sJDBC_COLUMN_USAGE, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_REMARKS, java.sql.Types.VARCHAR);
    rsh.addColumn(sJDBC_CHAR_OCTET_LENGTH, java.sql.Types.INTEGER); // max bytes in character
    rsh.addColumn(sJDBC_IS_NULLABLE, java.sql.Types.VARCHAR); // YES, NO or empty
    List<Row> listPseudoColumns = new ArrayList<Row>();
    Collections.sort(listPseudoColumns,new PseudoColumnsComparator());
    MetaDataCursor mdc = new MetaDataCursor(listPseudoColumns);
    AccessResultSet rs = new AccessResultSet(_conn,null,rsh,mdc);
    return rs;
  } /* getPseudoColumns */

} /* AcessDatabaseMetaData */
