package ch.admin.bar.siard2.access.expression;

import ch.admin.bar.siard2.access.*;
import ch.enterag.sqlparser.expression.*;
import ch.enterag.sqlparser.identifier.*;

public class AccessQuerySpecification
  extends QuerySpecification
{
  private boolean equalIgnoreCase(String s1, String s2)
  {
    boolean bEqual = true;
    if (s1 != null)
    {
      if (!s1.equalsIgnoreCase(s2))
        bEqual = false;
    }
    else if (s2 != null)
      bEqual = false;
    return bEqual;
  } /* equalIgnoreCase */
  
  /*------------------------------------------------------------------*/
  @Override
  protected boolean equalTables(QualifiedId qiTable1, QualifiedId qiTable2)
  {
    return equalIgnoreCase(qiTable1.getName(),qiTable2.getName()) &&
    equalIgnoreCase(qiTable1.getSchema(),qiTable2.getSchema()) &&
    equalIgnoreCase(qiTable1.getCatalog(),qiTable2.getCatalog());
  } /* equalTables */
  

  public AccessQuerySpecification(AccessSqlFactory sf)
  {
    super(sf);
  } /* constructor */

}
