package ch.admin.bar.siard2.access;

import java.io.*;
import java.math.*;
import java.sql.*;
import java.sql.Date;
import java.text.*;
import java.util.*;
import ch.enterag.utils.*;
import ch.enterag.utils.base.*;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.datatype.*;
import ch.enterag.sqlparser.ddl.*;
import ch.enterag.sqlparser.ddl.enums.*;
import ch.enterag.sqlparser.dml.*;
import ch.enterag.sqlparser.expression.*;
import ch.enterag.sqlparser.expression.enums.*;
import ch.enterag.sqlparser.identifier.*;

@SuppressWarnings("deprecation")
public class TestSqlDatabase
{
  public static final String _sTEST_SCHEMA = null;
  private static final String _sTEST_TABLE_SIMPLE = "TSQLSIMPLE";
  public static QualifiedId getQualifiedSimpleTable() { return new QualifiedId(null,_sTEST_SCHEMA,_sTEST_TABLE_SIMPLE); }
  public static int _iPrimarySimple = -1;
  private static List<TestColumnDefinition> getListCdSimple()
  {
    List<TestColumnDefinition> listCdSimple = new ArrayList<TestColumnDefinition>();
    listCdSimple.add(new TestColumnDefinition("CCHAR_5","CHAR(5)","Abcd"));
    listCdSimple.add(new TestColumnDefinition("CVARCHAR_255","VARCHAR(255)",TestUtils.getString(196)));
    listCdSimple.add(new TestColumnDefinition("CCLOB_2M","CLOB(2M)",TestUtils.getString(2000000)));
    listCdSimple.add(new TestColumnDefinition("CNCHAR_5","NCHAR(5)","Niño"));
    listCdSimple.add(new TestColumnDefinition("CNVARCHAR_127","NCHAR VARYING(127)",TestUtils.getNString(100)));
    listCdSimple.add(new TestColumnDefinition("CNCLOB_1M","NCLOB(1M)",TestUtils.getNString(1000000)));
    listCdSimple.add(new TestColumnDefinition("CXML","XML","<a>foöäpwkfèégopàèwerkgvoperkv &lt; and &amp; ifjeifj</a>"));
    listCdSimple.add(new TestColumnDefinition("CBINARY_5","BINARY(5)",new byte[] {1,-2,3,-4} ));
    listCdSimple.add(new TestColumnDefinition("CVARBINARY_255","VARBINARY(255)",TestUtils.getBytes(196) ));
    listCdSimple.add(new TestColumnDefinition("CBLOB","BLOB",TestUtils.getBytes(1000000)));
    listCdSimple.add(new TestColumnDefinition("CNUMERIC_28","NUMERIC(28)",BigInteger.valueOf(1234567890123456789l)));
    listCdSimple.add(new TestColumnDefinition("CDECIMAL_15_5","DECIMAL(15,5)",new BigDecimal(BigInteger.valueOf(3141592653210l),5)));
    listCdSimple.add(new TestColumnDefinition("CSMALLINT","SMALLINT",new Short((short)-32000)));
    _iPrimarySimple = listCdSimple.size(); // next column will be primary key column 
    listCdSimple.add(new TestColumnDefinition("CINTEGER","INTEGER",new Integer(1234567890)));
    listCdSimple.add(new TestColumnDefinition("CBIGINT","BIGINT",new Long(-123456789012345678l)));
    listCdSimple.add(new TestColumnDefinition("CFLOAT_10","FLOAT(10)",new Double(Math.E)));
    listCdSimple.add(new TestColumnDefinition("CREAL","REAL",new Float(Math.PI)));
    listCdSimple.add(new TestColumnDefinition("CDOUBLE","DOUBLE PRECISION",new Double(Math.E)));
    listCdSimple.add(new TestColumnDefinition("CBOOLEAN","BOOLEAN",new Boolean(true)));
    listCdSimple.add(new TestColumnDefinition("CDATE","DATE",new Date(2016-1900,11,28)));
    listCdSimple.add(new TestColumnDefinition("CTIME","TIME",new Time(13,45,28)));
    listCdSimple.add(new TestColumnDefinition("CTIMESTAMP","TIMESTAMP(3)",new Timestamp(2016-1900,11,28,13,45,28,123000000)));
    listCdSimple.add(new TestColumnDefinition("CINTERVAL_YEAR_2_MONTH","INTERVAL YEAR(2) TO MONTH",new Interval(1,123,3)));
    listCdSimple.add(new TestColumnDefinition("CINTERVAL_DAY_2_SECONDS_6","INTERVAL DAY(2) TO SECOND(6)",new Interval(1,4,17,54,23,123456000l)));
    return listCdSimple;
  }
  public static List<TestColumnDefinition> _listCdSimple = getListCdSimple();

  private SqlFactory _sf = new BaseSqlFactory();
  private Connection _conn = null;

  /*------------------------------------------------------------------*/
  private void rollback(String sMessage, SQLException se)
  {
    System.out.println(sMessage+": "+EU.getExceptionMessage(se));
      try { _conn.rollback(); }
      catch(SQLException seRollback) 
      {
        System.err.println("Rollback failed: " +
          EU.getExceptionMessage(seRollback)); 
      }
  } /* rollback */

  /*------------------------------------------------------------------*/
  public TestSqlDatabase(Connection conn)
    throws SQLException
  {
    _conn = conn;
    drop();
    create();
    _conn.commit();
  } /* constructor TestSqlDatabase */
  
  /*------------------------------------------------------------------*/
  private void drop()
  {
    dropTables();
  } /* drop */
  
  /*------------------------------------------------------------------*/
  private void dropTable(QualifiedId qiTable)
  {
    try
    {
      DropTableStatement dts = _sf.newDropTableStatement();
      dts.initialize(qiTable,DropBehavior.RESTRICT);
      Statement stmt = _conn.createStatement();
      int iResult = stmt.executeUpdate(dts.format());
      if (iResult != 0)
        throw new SQLException("Drop table "+qiTable.format()+" failed!");
      stmt.close();
      _conn.commit();
    }
    catch (SQLException se) { rollback("Drop table "+qiTable.format(), se); }
  } /* dropTable */

  /*------------------------------------------------------------------*/
  private void dropTables()
  {
    dropTable(getQualifiedSimpleTable());
  } /* dropTables */

  /*------------------------------------------------------------------*/
  private void create()
    throws SQLException
  {
    createTables();
    insertTables();
  } /* create */
  
  /*------------------------------------------------------------------*/
  private TableElement getTableElement(TestColumnDefinition tcd, boolean bNotNull)
    throws SQLException
  {
    DataType dt = _sf.newDataType();
    if (tcd.getValue() instanceof List<?>)
    {
      try 
      { 
        QualifiedId qiType = new QualifiedId(tcd.getType());
        dt.initStructType(qiType);
      }
      catch(ParseException pe) { throw new SQLException("Type name "+tcd.getType()+" could not be parsed!",pe); }
    }
    else
    {
      PredefinedType pt = null;
      try
      {
        pt = _sf.newPredefinedType();
        pt.parse(tcd.getType());
        dt.initPredefinedDataType(pt);
      }
      catch(IllegalArgumentException iae) {}
      if (pt.getType() == null)
      {
        try 
        { 
          QualifiedId qiType = new QualifiedId(tcd.getType());
          dt.initStructType(qiType);
        }
        catch(ParseException pe) { throw new SQLException("Type name "+tcd.getType()+" could not be parsed!",pe); }
      }
    }
    ColumnDefinition cd = _sf.newColumnDefinition();
    if (bNotNull)
    {
      ColumnConstraintDefinition ccd = cd.getColumnConstraintDefinition();
      if (ccd == null)
      {
        ccd = _sf.newColumnConstraintDefinition();
        cd.setColumnConstraintDefinition(ccd);
      }
      ccd.setType(ColumnConstraintType.NOT_NULL);
    }
    cd.initNameType(new Identifier(tcd.getName()), dt);
    TableElement te = _sf.newTableElement();
    te.initColumnDefinition(cd);
    return te;
  } /* getTableElement */
  
  /*------------------------------------------------------------------*/
  private TableElement getPrimaryTableElement(String sPkName, List<TestColumnDefinition> listCdPrimary)
  {
    IdList ilPrimary = new IdList();
    for (int i = 0; i < listCdPrimary.size(); i++)
    {
      TestColumnDefinition tcd = listCdPrimary.get(i);
      ilPrimary.get().add(tcd.getName());
    }
    TableConstraintDefinition tcd = _sf.newTableConstraintDefinition();
    tcd.initPrimaryKey(new QualifiedId(null,null,sPkName), ilPrimary);
    TableElement te = _sf.newTableElement();
    te.initTableConstraintDefinition(tcd);
    return te;
  } /* getPrimaryTableElement */

  /*------------------------------------------------------------------*/
  private TableElement getForeignTableElement(String sFkName, List<TestColumnDefinition> listCdForeign,
    QualifiedId qiTableReferenced, List<TestColumnDefinition> listCdReferenced)
  {
    IdList ilForeign = new IdList();
    for (int i = 0; i < listCdForeign.size();i++)
    {
      TestColumnDefinition tcd = listCdForeign.get(i);
      ilForeign.get().add(tcd.getName());
    }
    IdList ilReferenced = new IdList();
    for (int i = 0; i < listCdReferenced.size();i++)
    {
      TestColumnDefinition tcd = listCdReferenced.get(i);
      ilReferenced.get().add(tcd.getName());
    }
    TableConstraintDefinition tcd = _sf.newTableConstraintDefinition();
    tcd.initialize(new QualifiedId(null,null,sFkName), TableConstraintType.FOREIGN_KEY,
      ilForeign, qiTableReferenced, ilReferenced, null, ReferentialAction.CASCADE, 
      null, null, null, null);
    TableElement te = _sf.newTableElement();
    te.initTableConstraintDefinition(tcd);
    return te;
  } /* getForeignTableElement */
  
  /*------------------------------------------------------------------*/
  private void createTable(QualifiedId qiTable, List<TestColumnDefinition> listCd,
    List<TestColumnDefinition> listCdPrimary, List<TestColumnDefinition> listCdForeign,
    QualifiedId qiTableReferenced, List<TestColumnDefinition> listCdReferenced)
    throws SQLException
  {
    CreateTableStatement cts = _sf.newCreateTableStatement();
    List<TableElement> listTableElements = new ArrayList<TableElement>();
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      TestColumnDefinition tcd = listCd.get(iColumn);
      listTableElements.add(getTableElement(tcd, listCdPrimary.contains(tcd)));
    }
    listTableElements.add(getPrimaryTableElement("PK"+qiTable.getName(), listCdPrimary));
    if (listCdForeign != null)
      listTableElements.add(getForeignTableElement("FK"+qiTable.getName(),
        listCdForeign,qiTableReferenced,listCdReferenced));
    cts.initTableElements(null,
      qiTable,
      listTableElements,null);
    // System.out.println(cts.format());
    Statement stmt = _conn.createStatement();
    int iResult = stmt.executeUpdate(cts.format());
    if (iResult != 0)
      throw new SQLException("Create table "+qiTable.format()+" failed!");
    stmt.close();
    _conn.commit();
  } /* createTable */
  
  /*------------------------------------------------------------------*/
  private List<TestColumnDefinition> getSingle(TestColumnDefinition tcd)
  {
    List<TestColumnDefinition> listSingle = 
      Arrays.asList(new TestColumnDefinition[]{tcd});
    return listSingle;
  } /* getSingle */
  
  /*------------------------------------------------------------------*/
  private void createTables()
    throws SQLException
  {
    createTable(getQualifiedSimpleTable(),_listCdSimple,
      getSingle(_listCdSimple.get(_iPrimarySimple)),
      null,null,null);
  } /* createTables */

  /*------------------------------------------------------------------*/
  private CommonValueExpression getCommonValueExpression()
  {
    ValueExpressionPrimary vep = _sf.newValueExpressionPrimary();
    CommonValueExpression cve = _sf.newCommonValueExpression();
    cve.initialize(null, null, null, null, null, null, vep);
    return cve;
  } /* getCommonValueExpressionArray */

  /*------------------------------------------------------------------*/
  private CommonValueExpression getCommonValueExpressionLiteral()
  {
    UnsignedLiteral ul = _sf.newUnsignedLiteral();
    ValueExpressionPrimary vep = _sf.newValueExpressionPrimary();
    vep.initUnsignedLiteral(ul);
    CommonValueExpression cve = _sf.newCommonValueExpression();
    cve.initialize(null, null, null, null, null, null, vep);
    return cve;
  } /* getCommonValueExpression */
  
  /*------------------------------------------------------------------*/
  private CommonValueExpression getCommonValueExpressionDynamic()
  {
    GeneralValueSpecification gvs = _sf.newGeneralValueSpecification();
    gvs.initialize(new ColonId(), new ColonId(), new IdChain(), GeneralValue.QUESTION_MARK);
    CommonValueExpression cve = getCommonValueExpression();
    cve.getValueExpressionPrimary().setGeneralValueSpecification(gvs);
    return cve;
  } /* getCommonValueExpressionDynamic */
  
  /*------------------------------------------------------------------*/
  private CommonValueExpression getCommonValueExpressionNumeric()
  {
    UnsignedLiteral ul = _sf.newUnsignedLiteral();
    ValueExpressionPrimary vep = _sf.newValueExpressionPrimary();
    vep.initUnsignedLiteral(ul);
    NumericValueExpression nve = _sf.newNumericValueExpression();
    nve.initialize(null, null, null, null, null, null, vep, null);
    CommonValueExpression cve = _sf.newCommonValueExpression();
    cve.initialize(nve, null, null, null, null, null, null);
    return cve;
  } /* getCommonValueExpressionNumeric */
  
  /*------------------------------------------------------------------*/
  private ValueExpression getValueExpression(TestColumnDefinition tcd, 
    List<TestColumnDefinition> listLobs)
    throws SQLException
  {
    ValueExpression ve = _sf.newValueExpression();
    CommonValueExpression cve = null;
    Object o = tcd.getValue();
    if ((o instanceof String) ||
      (o instanceof byte[]) ||
      (o instanceof Boolean) ||
      (o instanceof Date) ||
      (o instanceof Time) ||
      (o instanceof Timestamp) ||
      (o instanceof Interval))
      cve = getCommonValueExpressionLiteral();
    else if ((o instanceof BigDecimal) ||
             (o instanceof BigInteger) ||
             (o instanceof Short) ||
             (o instanceof Integer) ||
             (o instanceof Long) ||
             (o instanceof Float) ||
             (o instanceof Double))
      cve = getCommonValueExpressionNumeric();
    else if (o instanceof List<?>)
      cve = getCommonValueExpression();
    
    if (o instanceof String)
    {
      String s = (String)o;
      if (s.length() < 256)
        cve.getValueExpressionPrimary().getUnsignedLit().
          initCharacterString(s);
      else
      {
        cve = getCommonValueExpressionDynamic();
        listLobs.add(tcd);
      }
    }
    else if (o instanceof byte[])
    {
      byte[] buf = (byte[])o;
      if (buf.length < 256)
        cve.getValueExpressionPrimary().getUnsignedLit().
          initBytes(buf);
      else
      {
        cve = getCommonValueExpressionDynamic();
        listLobs.add(tcd);
      }
    }
    else if (o instanceof BigDecimal)
    {
      BigDecimal bd = (BigDecimal)o;
      cve.getNumericValueExpression().getValueExpressionPrimary().getUnsignedLit().
        initBigDecimal(bd.abs());
    if (bd.signum() < 0)
      cve.getNumericValueExpression().
        setSign(Sign.MINUS_SIGN);
    }
    else if (o instanceof BigInteger)
    {
      BigInteger bi = (BigInteger)o;
      BigDecimal bd = new BigDecimal(bi);
      cve.getNumericValueExpression().getValueExpressionPrimary().getUnsignedLit().
        initBigDecimal(bd.abs());
    if (bd.signum() < 0)
      cve.getNumericValueExpression().
        setSign(Sign.MINUS_SIGN);
    }
    else if (o instanceof Short)
    {
      short sh = ((Short)o).shortValue();
      cve.getNumericValueExpression().getValueExpressionPrimary().getUnsignedLit().
        initInteger(Math.abs(sh));
      if (sh < 0)
        cve.getNumericValueExpression().
          setSign(Sign.MINUS_SIGN);
    }
    else if (o instanceof Integer)
    {
      int i = ((Integer)o).intValue();
      cve.getNumericValueExpression().getValueExpressionPrimary().getUnsignedLit().
        initInteger(Math.abs(i));
      if (i < 0)
        cve.getNumericValueExpression().
          setSign(Sign.MINUS_SIGN);
    }
    else if (o instanceof Long)
    {
      long l = ((Long)o).longValue();
      cve.getNumericValueExpression().getValueExpressionPrimary().getUnsignedLit().
        initLong(Math.abs(l));
      if (l < 0)
        cve.getNumericValueExpression().
          setSign(Sign.MINUS_SIGN);
    }
    else if (o instanceof Float)
    {
      double d = ((Float)o).doubleValue();
      cve.getNumericValueExpression().getValueExpressionPrimary().getUnsignedLit().
        initDouble(Math.abs(d));
      if (d < 0)
        cve.getNumericValueExpression().
          setSign(Sign.MINUS_SIGN);
    }
    else if (o instanceof Double)
    {
      double d = ((Double)o).doubleValue();
      cve.getNumericValueExpression().getValueExpressionPrimary().getUnsignedLit().
        initDouble(Math.abs(d));
      if (d < 0)
        cve.getNumericValueExpression().
          setSign(Sign.MINUS_SIGN);
    }
    else if (o instanceof Boolean)
    {
      boolean b = ((Boolean)o).booleanValue();
      cve.getValueExpressionPrimary().getUnsignedLit().
        initBoolean(b? BooleanLiteral.TRUE: BooleanLiteral.FALSE);
    }
    else if (o instanceof Date)
    {
      cve.getValueExpressionPrimary().getUnsignedLit().
        initDate((Date)o);
    }
    else if (o instanceof Time)
    {
      cve.getValueExpressionPrimary().getUnsignedLit().
        initTime((Time)o);
    }
    else if (o instanceof Timestamp)
    {
      cve.getValueExpressionPrimary().getUnsignedLit().
        initTimestamp((Timestamp)o);
    }
    else if (o instanceof Interval)
    {
      Interval iv = (Interval)o;
      cve.getValueExpressionPrimary().getUnsignedLit().initInterval(iv);
    }
    else if (o instanceof List<?>)
    {
      try
      {
        QualifiedId qiType = new QualifiedId(tcd.getType()); 
        @SuppressWarnings("unchecked")
        List<TestColumnDefinition> listAd = (List<TestColumnDefinition>)o;
        List<SqlArgument> listAttributeValues = new ArrayList<SqlArgument>();
        for (int iAttribute = 0; iAttribute < listAd.size(); iAttribute++)
        {
          TestColumnDefinition tcdAttribute = listAd.get(iAttribute);
          listAttributeValues.add(getAttributeValue(tcdAttribute,listLobs));
        }
        cve.getValueExpressionPrimary().
          initUdtValueConstructor(qiType,listAttributeValues);
      }
      catch(ParseException pe) { throw new SQLException("Type name "+tcd.getType()+" could not be parsed!",pe); }
    }
    else
      throw new SQLException("Unexpected value type "+o.getClass().getName()+"!");
    ve.initialize(cve, null, null);
    return ve;
  } /* getValueExpression */
  
  /*------------------------------------------------------------------*/
  private SqlArgument getAttributeValue(TestColumnDefinition tcd, 
    List<TestColumnDefinition> listLobs)
    throws SQLException
  {
    SqlArgument sa = _sf.newSqlArgument();
    ValueExpression ve = getValueExpression(tcd, listLobs);
    sa.initialize(ve, new QualifiedId(), null);
    return sa;
  } /* getAttributeValue */
  
  /*------------------------------------------------------------------*/
  private UpdateSource getUpdateSource(TestColumnDefinition tcd, 
    List<TestColumnDefinition> listLobs)
    throws SQLException
  {
    ValueExpression ve = getValueExpression(tcd,listLobs);
    UpdateSource us = _sf.newUpdateSource();
    us.initialize(ve, null);
    return us;
  } /* getUpdateSource */
  
  /*------------------------------------------------------------------*/
  private void insertTable(QualifiedId qiTable, List<TestColumnDefinition> listCd)
    throws SQLException
  {
    IdList ilColumns = new IdList();
    List<AssignedRow> listValues = new ArrayList<AssignedRow>();
    AssignedRow ar = _sf.newAssignedRow();
    listValues.add(ar);
    List<TestColumnDefinition> listLobs = new ArrayList<TestColumnDefinition>();
    for (int iColumn = 0; iColumn < listCd.size(); iColumn++)
    {
      TestColumnDefinition tcd = listCd.get(iColumn);
      ilColumns.get().add(tcd.getName());
      ar.getUpdateSources().add(getUpdateSource(tcd,listLobs));
    }
    InsertStatement is = _sf.newInsertStatement();
    is.initialize(qiTable, ilColumns, listValues, 
      null, null, false);
    // System.out.println(is.format());
    PreparedStatement pstmt = _conn.prepareStatement(is.format());
    for (int iLob = 0; iLob < listLobs.size();iLob++)
    {
      TestColumnDefinition tcd = listLobs.get(iLob);
      if (tcd.getValue() instanceof String)
      {
        Reader rdrClob = new StringReader((String)tcd.getValue());
        pstmt.setCharacterStream(iLob+1, rdrClob);
      }
      else if (tcd.getValue() instanceof byte[])
      {
        InputStream isBlob = new ByteArrayInputStream((byte[])tcd.getValue());
        pstmt.setBinaryStream(iLob+1, isBlob);
      }
      else
        throw new SQLException("Invalid LOB!");
    }
    int iResult = pstmt.executeUpdate();
    if (iResult != 1)
      throw new SQLException("Insert into table "+qiTable.format()+" failed!");
    pstmt.close();
    _conn.commit();  
  } /* insertTable */
  
  /*------------------------------------------------------------------*/
  private void insertTables()
    throws SQLException
  {
    insertTable(getQualifiedSimpleTable(),_listCdSimple);
  } /* insertTables */
  
} /* class TestSqlDatabase */
