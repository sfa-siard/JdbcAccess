/*======================================================================
A primitive implementation of Row.
Application : Access JDBC driver
Description : Primitive row implementation.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 07.11.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.access;

import java.io.*;
import java.math.*;
import java.util.*;
import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.complex.*;
import com.healthmarketscience.jackcess.util.*;

public class ResultSetRow 
  extends HashMap<String,Object> 
  implements Row
{
  /** serial number */
  private static final long serialVersionUID = -6968202854661574845L;
  /** row id */
  private RowId _rowId = null;
  
  /** RowId implementation using UUID */
  private class ResultSetRowId implements RowId
  {
    /** id */
    UUID _id = null;
    /** @return id */
    UUID getId() { return _id; } 
    
    /** constructor */
    ResultSetRowId()
    {
      _id = UUID.randomUUID();
    }

    /** compare this row to another */
    @Override
    public int compareTo(RowId riOther)
    {
      ResultSetRowId mriOther = (ResultSetRowId) riOther;
      return _id.toString().compareTo(mriOther.getId().toString());
    }
  } /* class MetaDataRowId */

  /*------------------------------------------------------------------*/
  /** constructor creates an empty row. 
   */
  public ResultSetRow()
  {
    super();
    _rowId = new ResultSetRowId();    
  } /* constructor MetaDataRow */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Object put(String key, Object value)
  {
    return super.put(key,value);
  } /* put */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public RowId getId()
  {
    return _rowId;
  } /* getId */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public String getString(String name)
  {
    return (String)get(name);
  } /* getString */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Boolean getBoolean(String name)
  {
    return (Boolean)get(name);
  } /* getBoolean */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Byte getByte(String name)
  {
    return (Byte)get(name);
  } /* getByte */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Short getShort(String name)
  {
    return (Short)get(name);
  } /* getShort */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Integer getInt(String name)
  {
    return (Integer)get(name);
  } /* getInt */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public BigDecimal getBigDecimal(String name)
  {
    return (BigDecimal)get(name);
  } /* getBigDecimal */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Float getFloat(String name)
  {
    return (Float)get(name);
  } /* getFloat */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Double getDouble(String name)
  {
    return (Double)get(name);
  } /* getDouble */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public Date getDate(String name)
  {
    return (Date)get(name);
  } /* getDate */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public byte[] getBytes(String name)
  {
    return (byte[])get(name);
  } /* getBytes */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public ComplexValueForeignKey getForeignKey(String name)
  {
    return (ComplexValueForeignKey)get(name);
  } /* getForeignKey */

  /*------------------------------------------------------------------*/
  /** {@link Row} */
  @Override
  public OleBlob getBlob(String name) throws IOException
  {
    return (OleBlob)get(name);
  } /* getBlob */

} /* ResultSetRow */
