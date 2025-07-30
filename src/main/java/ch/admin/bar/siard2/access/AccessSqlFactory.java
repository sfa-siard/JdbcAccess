/*======================================================================
AccessSqlFactory overrides the BaseSqlFactory for the Access-specific SQL 
parser classes.
Application : SIARD2
Description : AccessSqlFactory overrides the BaseSqlFactory for the 
              Access-specific SQL parser classes.
Platform    : Java 7   
------------------------------------------------------------------------
Copyright  : 2016, Enter AG, Rüti ZH, Switzerland
Created    : 04.11.2016, Hartwig Thomas
======================================================================*/
package ch.admin.bar.siard2.access;

import ch.admin.bar.siard2.access.datatype.AccessPredefinedType;
import ch.admin.bar.siard2.access.expression.*;
import ch.enterag.sqlparser.BaseSqlFactory;
import ch.enterag.sqlparser.SqlFactory;
import ch.enterag.sqlparser.SqlStatement;
import ch.enterag.sqlparser.datatype.PredefinedType;
import ch.enterag.sqlparser.expression.*;

import java.util.ArrayList;
import java.util.List;

/*====================================================================*/

/** AccessSqlFactory overrides the BaseSqlFactory for the Access-specific
 * SQL parser classes.
 * @author Hartwig Thomas
 */
public class AccessSqlFactory extends BaseSqlFactory implements SqlFactory {
    private final List<GeneralValueSpecification> _listQuestionMarks = new ArrayList<GeneralValueSpecification>();

    public List<GeneralValueSpecification> getQuestionMarks() {
        return _listQuestionMarks;
    }

    @Override
    public SqlStatement newSqlStatement() {
        return new AccessSqlStatement(this);
    } /* newSqlStatement */

    @Override
    public ValueExpressionPrimary newValueExpressionPrimary() {
        return new AccessValueExpressionPrimary(this);
    } /* newValueExpressionPrimary */

    @Override
    public TablePrimary newTablePrimary() {
        return new AccessTablePrimary(this);
    } /* newTablePrimary */

    @Override
    public QuerySpecification newQuerySpecification() {
        return new AccessQuerySpecification(this);
    } /* newQuerySpecification */

    @Override
    public GeneralValueSpecification newGeneralValueSpecification() {
        return new AccessGeneralValueSpecification(this);
    } /* newGeneralValueSpecification */

    @Override
    public PredefinedType newPredefinedType() {
        return new AccessPredefinedType(this);
    } /* newPredefinedType */

    @Override
    public UnsignedLiteral newUnsignedLiteral() {
        return new AccessUnsignedLiteral(this);
    } /* newUnsignedLiteral */

} /* AccessSqlFactory */
