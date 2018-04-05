package ch.admin.bar.siard2.access.expression;

import ch.admin.bar.siard2.access.*;
import ch.enterag.sqlparser.expression.*;
import ch.enterag.sqlparser.expression.enums.*;

public class AccessGeneralValueSpecification
  extends GeneralValueSpecification
{
  @Override
  public void setGeneralValue(GeneralValue generalValue) 
  { 
    super.setGeneralValue(generalValue);
    if (generalValue == GeneralValue.QUESTION_MARK)
      ((AccessSqlFactory)getSqlFactory()).getQuestionMarks().add(this);
  } /* setGeneralValue */
  
  public AccessGeneralValueSpecification(AccessSqlFactory sf)
  {
    super(sf);
  } /* constructor */

} /* AccessGeneralValueSpecification */
