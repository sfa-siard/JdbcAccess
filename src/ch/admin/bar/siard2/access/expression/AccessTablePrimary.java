package ch.admin.bar.siard2.access.expression;

import java.util.*;
import ch.admin.bar.siard2.access.*;
import ch.enterag.sqlparser.datatype.*;
import ch.enterag.sqlparser.expression.*;

public class AccessTablePrimary
  extends TablePrimary
{

  /*------------------------------------------------------------------*/
  @Override
  public DataType getColumnType(String sColumnName)
  {
    DataType dt = null;
    for (Iterator<String> iterColumn = getColumnTypes().keySet().iterator(); 
      (dt == null) && iterColumn.hasNext(); )
    {
      String sColumn = iterColumn.next();
      if (sColumnName.equalsIgnoreCase(sColumn))
        dt = getColumnTypes().get(sColumn);
    }
    return dt;
  } /* getColumnType */
  
  /*------------------------------------------------------------------*/
  @Override
  public boolean hasColumn(String sColumn)
  {
    boolean bHasColumn = false;
    Map<String,DataType> mapColumnTypes = getColumnTypes();
    for (Iterator<String> iterColumn = mapColumnTypes.keySet().iterator(); 
      (!bHasColumn) && iterColumn.hasNext(); )
    {
      String sColumnName = iterColumn.next();
      if (sColumnName.equalsIgnoreCase(sColumn))
        bHasColumn = true;
    }
    return bHasColumn;
  } /* hasColumn */
  
  /*------------------------------------------------------------------*/
  public AccessTablePrimary(AccessSqlFactory sf)
  {
    super(sf);
  } /* constructor */

}
