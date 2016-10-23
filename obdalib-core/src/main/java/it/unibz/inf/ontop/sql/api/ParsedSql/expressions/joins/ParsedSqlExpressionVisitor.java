package it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins;

import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlAttribute;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlCondition;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlExpression;

/**
 * Created by Salvatore Rapisarda on 30/09/2016.
 *
 */
public interface ParsedSqlExpressionVisitor {
    void visit(PSqlAttribute var1);
    void visit(PSqlInnerJoinUsing var1);
    void visit(PSqlCondition var1);
    void visit(PSqlExpression var1);
}
