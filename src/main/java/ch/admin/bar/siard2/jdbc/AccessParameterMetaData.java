package ch.admin.bar.siard2.jdbc;

import java.math.*;
import java.sql.*;
import javax.xml.datatype.*;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.datatype.enums.*;
import ch.enterag.sqlparser.datatype.*;
import ch.enterag.sqlparser.expression.*;

import ch.admin.bar.siard2.access.AccessSqlFactory;

public class AccessParameterMetaData
  implements ParameterMetaData
{
  AccessSqlFactory _sf = null;
  SqlStatement _sqlstmt = null;
  
  public AccessParameterMetaData(AccessSqlFactory sf, SqlStatement sqlstmt)
  {
    _sf = sf;
    _sqlstmt = sqlstmt;
  } /* constructor */
  
  @Override
  public int getParameterCount() throws SQLException
  {
    return _sf.getQuestionMarks().size();
  } /* getParameterCount */

  @Override
  public int getParameterType(int param) throws SQLException
  {
    GeneralValueSpecification gvs = _sf.getQuestionMarks().get(param-1);
    return _sqlstmt.getGeneralType(gvs).getPredefinedType().getType().getSqlType();
  } /* getParameterType */

  @Override
  public String getParameterTypeName(int param) throws SQLException
  {
    GeneralValueSpecification gvs = _sf.getQuestionMarks().get(param-1);
    return _sqlstmt.getGeneralType(gvs).getPredefinedType().getType().getKeyword();
  } /* getParameterTypeName */ 

  @Override
  public int getPrecision(int param) throws SQLException
  {
    GeneralValueSpecification gvs = _sf.getQuestionMarks().get(param-1);
    PredefinedType pt = _sqlstmt.getGeneralType(gvs).getPredefinedType();
    int iPrecision = pt.getPrecision();
    if (iPrecision == PredefinedType.iUNDEFINED)
      iPrecision = pt.getLength();
    if (pt.getMultiplier() != null)
      iPrecision = iPrecision*pt.getMultiplier().getValue();
    return iPrecision;
  } /* getPrecision */

  @Override
  public int getScale(int param) throws SQLException
  {
    GeneralValueSpecification gvs = _sf.getQuestionMarks().get(param-1);
    PredefinedType pt = _sqlstmt.getGeneralType(gvs).getPredefinedType();
    int iScale = pt.getScale();
    if (iScale == PredefinedType.iUNDEFINED)
      iScale = pt.getSecondsDecimals();
    return iScale;
  } /* getScale */

  @Override
  public String getParameterClassName(int param) throws SQLException
  {
    Class<?> cls = null; 
    GeneralValueSpecification gvs = _sf.getQuestionMarks().get(param-1);
    PreType pt = _sqlstmt.getGeneralType(gvs).getPredefinedType().getType();
    switch(pt)
    {
      case NCHAR:
      case NVARCHAR:
      case CHAR:
      case VARCHAR: cls = String.class; break;
      case NCLOB:
      case CLOB: cls = Clob.class; break;
      case BINARY:
      case VARBINARY: cls = byte[].class; break;
      case BLOB:
      case DATALINK: cls = Blob.class; break;
      case NUMERIC:
      case DECIMAL: cls = BigDecimal.class; break;
      case SMALLINT: cls = Short.class; break;
      case INTEGER: cls = Integer.class; break;
      case BIGINT: cls = Long.class; break;
      case DOUBLE:
      case FLOAT: cls = Double.class; break;
      case REAL: cls = Float.class; break;
      case BOOLEAN: cls = Boolean.class; break;
      case DATE: cls = Date.class; break;
      case TIME: cls = Time.class; break;
      case TIMESTAMP: cls = Timestamp.class; break;
      case XML: cls = SQLXML.class; break;
      case INTERVAL: cls = Duration.class; break;
    }
    return cls.getName();
  } /* getParameterClassName */

  @Override
  public boolean isSigned(int param) throws SQLException
  {
    boolean bSigned = false;
    GeneralValueSpecification gvs = _sf.getQuestionMarks().get(param-1);
    PreType pt = _sqlstmt.getGeneralType(gvs).getPredefinedType().getType();
    switch(pt)
    {
      case DATE:
      case TIME:
      case TIMESTAMP:
      case XML:
      case BOOLEAN:
      case BLOB:
      case BINARY:
      case VARBINARY:
      case NCLOB:
      case CLOB:
      case NCHAR:
      case NVARCHAR:
      case CHAR:
      case VARCHAR:
      case DATALINK: bSigned = false; break;
      case INTERVAL:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case DOUBLE:
      case FLOAT:
      case REAL:
      case NUMERIC:
      case DECIMAL: bSigned = true; break;
    }
    return bSigned;
  } /* getSigned */

  @Override
  public int isNullable(int param) throws SQLException
  {
    return ParameterMetaData.parameterNullableUnknown;
  }

  @Override
  public int getParameterMode(int param) throws SQLException
  {
    return ParameterMetaData.parameterModeUnknown;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    return null;
  } /* unwrap */

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    return false;
  } /* isWrapperFor */

} /* AccessParameterMetaData */
