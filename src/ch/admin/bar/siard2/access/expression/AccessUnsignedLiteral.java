package ch.admin.bar.siard2.access.expression;

import java.math.*;

import ch.enterag.sqlparser.*;
import ch.enterag.sqlparser.expression.*;

public class AccessUnsignedLiteral
  extends UnsignedLiteral
{
  public static BigDecimal convertInterval(Interval interval)
  {
    BigDecimal bd = null;
    if (interval != null)
    {
      if ((interval.getYears() != 0) || (interval.getMonths() != 0))
      {
        long l = 12l*interval.getYears()+interval.getMonths();
        bd = BigDecimal.valueOf(l);
      }
      else
      {
        long l = interval.getNanoSeconds() + 
          1000000000l*(interval.getSeconds() +
            60*(interval.getMinutes()+
              60*(interval.getHours()+ 
                24*interval.getDays())));
        bd = BigDecimal.valueOf(l,9);
      }
    }
    return bd;
  } /* convertInterval */
  
  @Override
  public void setInterval(Interval interval)
  {
    super.setExact(convertInterval(interval));
  } /* setInterval */
  
  /*------------------------------------------------------------------*/
  /** format the unsigned literal
   * @return the MSSQL string corresponding to the fields of the unsigned literal.
   */
  @Override
  public String format()
  {
    String sFormatted = null;
    Interval ivValue = getInterval();
    if (ivValue != null)
    {
      if ((ivValue.getYears() != 0) || (ivValue.getMonths() != 0))
      {
        Long l = ((long)ivValue.getYears())*12+ivValue.getMonths();
        sFormatted = String.valueOf(l);
      }
      else
      {
        long l = ivValue.getNanoSeconds() + 
          1000000000l*(ivValue.getSeconds() +
            60*(ivValue.getMinutes()+
              60*(ivValue.getHours()+ 
                24*ivValue.getDays())));
        BigDecimal bd = new BigDecimal(BigInteger.valueOf(l),9);
        sFormatted = bd.toPlainString();
      }
    }
    else
      sFormatted = super.format();
    return sFormatted;
  } /* format */

  /*------------------------------------------------------------------*/
  /** constructor with factory only to be called by factory.
   * @param sf factory.
   */
  public AccessUnsignedLiteral(SqlFactory sf)
  {
    super(sf);
  } /* constructor */

} /* Db2UnsignedLiteral */
