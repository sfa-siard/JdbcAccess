package ch.admin.bar.siard2.jdbc;

import java.sql.*;
import java.util.*;
import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.datatype.*;
import ch.enterag.sqlparser.datatype.enums.*;

public class AccessArray implements Array
{
  private Object[] _ao = null;
  private PreType _pt = null; // only predefined types supported 
  
  public AccessArray(String sTypeName, Object[] ao)
  {
    _ao = ao;
    SqlFactory sf = new BaseSqlFactory();
    DataType dt = sf.newDataType();
    dt.parse(sTypeName);
    _pt = dt.getPredefinedType().getType();
  } /* constructor */
  
  @Override
  public void free() throws SQLException
  {
    _ao = null;
  } /* free */

  @Override
  public Object getArray() throws SQLException
  {
    return _ao;
  } /* getArray */

  @Override
  public Object getArray(Map<String, Class<?>> arg0)
    throws SQLException
  {
      throw new SQLFeatureNotSupportedException("Type mapping is not supported!");
  }

  @Override
  public Object getArray(long index, int count) throws SQLException
  {
    Object[] ao = null;
    if (_ao != null)
    {
      if (index <= _ao.length)
      {
        if (count > _ao.length - index)
          count = _ao.length - (int)index;
        ao = Arrays.copyOfRange(_ao, (int)index-1, (int)index+count-1);
      }
      else
        throw new IllegalArgumentException("Invalid start index!");
    }
    return ao;
  } /* getArray */

  @Override
  public Object getArray(long arg0, int arg1, Map<String, Class<?>> arg2)
    throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Type mapping is not supported!");
  }

  @Override
  public int getBaseType() throws SQLException
  {
    return _pt.getSqlType();
  } /* getBaseType */

  @Override
  public String getBaseTypeName() throws SQLException
  {
    return _pt.getKeyword();
  } /* getBaseTypeName */

  @Override
  public ResultSet getResultSet() throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported!");
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> arg0)
    throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported!");
  }

  @Override
  public ResultSet getResultSet(long arg0, int arg1)
    throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported!");
  }

  @Override
  public ResultSet getResultSet(long arg0, int arg1,
    Map<String, Class<?>> arg2) throws SQLException
  {
    throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported!");
  }

}
