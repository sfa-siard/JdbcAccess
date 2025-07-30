/*======================================================================
AccessPredefinedType implements the type translation from ISO SQL to MS Access.
Application : SIARD2
Description : AccessPredefinedType implements the type translation from 
              ISO SQL:2008 to MS Access. 
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2017, Enter AG, Rüti ZH, Switzerland
Created    : 15.03.2017, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.access.datatype;

import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.datatype.*;
import ch.enterag.sqlparser.datatype.enums.*;

/*====================================================================*/
/** AccessPredefinedType implements the type translation from ISO SQL to 
 * MS Access.
 * @author Hartwig Thomas
 */
public class AccessPredefinedType
  extends PredefinedType
{
  /*------------------------------------------------------------------*/
  /** map INTERVAL type to BIGINT (number of months) or leave it unchanged,
   * if IntervalQualifier has already beens set.
   * @param type predefined type.
   */ 
  @Override
  public void setType(PreType type)
  {
    if ((type == PreType.INTERVAL) && (getIntervalQualifier() == null))
      type = PreType.BIGINT;
    super.setType(type);
  } /* setType */
  
  /*------------------------------------------------------------------*/
  /** map INTERVAL type to BIGINT (number of months), if it is a YEAR-MONTH
   * interval, or to DECIMAL(19,9) (seconds) if it is a DAY-SECOND interval.
   * @param iq interval qualifier.
   */ 
  @Override
  public void setIntervalQualifier(IntervalQualifier iq)
  {
    if (iq != null)
    {
      if (iq.getStartField() != null)
      {
        if ((iq.getStartField() != IntervalField.YEAR) &&
          (iq.getStartField() != IntervalField.MONTH))
        {
          super.setType(PreType.DECIMAL);
          super.setPrecision(19);
          super.setScale(9);
        }
      }
      else 
        throw new IllegalArgumentException("Interval qualifier must have valid start field!");
    }
    else
      super.setIntervalQualifier(iq);
  } /* setIntervalQualifier */
  
  /*------------------------------------------------------------------*/
  /** constructor with factory only to be called by factory.
   * @param sf factory.
   */
  public AccessPredefinedType(SqlFactory sf)
  {
    super(sf);
  } /* constructor */

} /* class AccessPredefinedType */
