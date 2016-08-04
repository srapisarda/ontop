package it.unibz.inf.ontop.sql.api.visitors;

import com.google.common.collect.ImmutableList;
import com.sun.tools.javac.util.Pair;
import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.QuotedID;
import it.unibz.inf.ontop.sql.RelationID;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by salvo on 04/08/2016.
 */

class ParsedSQLExpressionVisitor implements ExpressionVisitor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> attributeAliasMap;
    private final ImmutableList.Builder<Column> columnsListBuilder;

    ParsedSQLExpressionVisitor() {
        this.attributeAliasMap = new HashMap<>();
        this.columnsListBuilder = new ImmutableList.Builder<>();
    }

    ImmutableList<Column> getColumns(){
        return columnsListBuilder.build();
    }

    @Override
    public void visit(NullValue nullValue) {
        logger.info("Visit NullValue");

    }

    @Override
    public void visit(Function function) {
        logger.info("Visit Function");
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        logger.info("Visit signedExpression");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        logger.info("Visit jdbcParameter");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        logger.info("Visit jdbcNamedParameter");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        logger.info("Visit doubleValue");
    }

    @Override
    public void visit(LongValue longValue) {
        logger.info("Visit longValue");
    }

    @Override
    public void visit(DateValue dateValue) {
        logger.info("Visit dateValue");
    }

    @Override
    public void visit(TimeValue timeValue) {
        logger.info("Visit timeValue");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        logger.info("Visit timestampValue");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        logger.info("Visit parenthesis");
    }

    @Override
    public void visit(StringValue stringValue) {
        logger.info("Visit stringValue");
    }

    @Override
    public void visit(Addition addition) {
        logger.info("Visit addition");
    }

    @Override
    public void visit(Division division) {
        logger.info("Visit division");
    }

    @Override
    public void visit(Multiplication multiplication) {
        logger.info("Visit multiplication");
    }

    @Override
    public void visit(Subtraction subtraction) {
        logger.info("Visit subtraction");
    }

    @Override
    public void visit(AndExpression andExpression) {
        logger.info("Visit andExpression");
    }

    @Override
    public void visit(OrExpression orExpression) {
        logger.info("Visit orExpression");
    }

    @Override
    public void visit(Between between) {
        logger.info("Visit Between");
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        logger.info("Visit EqualsTo");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        logger.info("Visit GreaterThan");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        logger.info("Visit GreaterThanEquals");
    }

    @Override
    public void visit(InExpression inExpression) {
        logger.info("Visit InExpression");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        logger.info("Visit IsNullExpression");
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        logger.info("Visit LikeExpression");
    }

    @Override
    public void visit(MinorThan minorThan) {
        logger.info("Visit MinorThan");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        logger.info("Visit MinorThanEquals");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        logger.info("Visit NotEqualsTo");
    }

    @Override
    public void visit(Column tableColumn) {
        logger.info("Visit Column ", tableColumn);
        columnsListBuilder.add(tableColumn);
        //tableColumn.accept(this);



    }

    @Override
    public void visit(SubSelect subSelect) {
        logger.info("Visit SubSelect");
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        logger.info("Visit CaseExpression");
    }

    @Override
    public void visit(WhenClause whenClause) {
        logger.info("Visit WhenClause");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        logger.info("Visit ExistsExpression");
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        logger.info("Visit AllComparisonExpression");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        logger.info("Visit AnyComparisonExpression");
    }

    @Override
    public void visit(Concat concat) {
        logger.info("Visit Concat");
    }

    @Override
    public void visit(Matches matches) {
        logger.info("Visit Matches");
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        logger.info("Visit BitwiseAnd");
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        logger.info("Visit BitwiseOr");
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        logger.info("Visit BitwiseXor");
    }

    @Override
    public void visit(CastExpression cast) {
        logger.info("Visit CastExpression");
    }

    @Override
    public void visit(Modulo modulo) {
        logger.info("Visit Modulo");
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        logger.info("Visit AnalyticExpression");
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        logger.info("Visit ExtractExpression");
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        logger.info("Visit IntervalExpression");
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        logger.info("Visit OracleHierarchicalExpression");
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        logger.info("Visit RegExpMatchOperator");
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        logger.info("Visit JsonExpression");
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        logger.info("Visit RegExpMySQLOperator");
    }
}
