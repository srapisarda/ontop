package it.unibz.inf.ontop.sql.api.ParsedSql.expressions;

import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.QuotedID;
import it.unibz.inf.ontop.sql.RelationID;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins.ParsedSqlExpressionVisitor;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlContext;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.OldOracleJoinBinaryExpression;
import net.sf.jsqlparser.schema.Column;

/**
 * Created by salvo on 25/09/2016.
 *
 */
public class PSqlCondition extends OldOracleJoinBinaryExpression implements PSqlExpression {

    private final OldOracleJoinBinaryExpression expression;
    private final PSqlContext context;
    private final PSqlAttribute rightAttribute;
    private final PSqlAttribute leftAttribute;

    public PSqlCondition(QualifiedAttributeID leftAttribute, QualifiedAttributeID rightAttribute) {
        this( new PSqlAttribute( leftAttribute), new PSqlAttribute( rightAttribute));
        super.setLeftExpression(this.leftAttribute);
        super.setRightExpression(this.rightAttribute);
    }



    public PSqlCondition(PSqlAttribute leftAttribute, PSqlAttribute rightAttribute) {
        this.context = null;
        this.leftAttribute = leftAttribute;
        this.rightAttribute = rightAttribute;
        this.expression = new EqualsTo();
        this.expression.setLeftExpression(this.leftAttribute  );
        this.expression.setRightExpression(this.rightAttribute  );
        super.setLeftExpression(leftAttribute);
        super.setRightExpression(rightAttribute);
    }

    public PSqlCondition(PSqlContext context, OldOracleJoinBinaryExpression expression) {
        this.expression = expression;
        this.context = context;


        PSqlExpressionVisitor visitor = new PSqlExpressionVisitor(context);
        expression.getLeftExpression().accept( visitor);
        if  ( ! visitor.getColumns().isEmpty() )
            this.leftAttribute =  getAttributeFromColumn ( visitor.getColumns().get(0) );
        else
            this.leftAttribute = new PSqlAttribute(expression.getLeftExpression().toString());


        visitor = new PSqlExpressionVisitor(context);
        expression.getRightExpression().accept( visitor);
        if  ( ! visitor.getColumns().isEmpty() )
            rightAttribute =  getAttributeFromColumn ( visitor.getColumns().get(0) );
        else
            rightAttribute = new PSqlAttribute(expression.getRightExpression().toString()); //new PSqlAttribute( new QualifiedAttributeID(null, context.getMetadata().getQuotedIDFactory().createAttributeID( expression.getRightExpression().toString())));

        super.setLeftExpression(leftAttribute);
        super.setRightExpression(rightAttribute);
    }


    private PSqlAttribute getAttributeFromColumn(Column column){
        final QuotedID attributeID = context.getMetadata().getQuotedIDFactory().createAttributeID(column.getColumnName());
        final RelationID relationID = context.getMetadata().getQuotedIDFactory().createRelationID(null, column.getTable().getName());
        QualifiedAttributeID qualifiedAttributeID = new QualifiedAttributeID(relationID, attributeID );
        return  new PSqlAttribute(qualifiedAttributeID);
    }

    public boolean isNot () {
        return  expression.isNot();
    }


    public PSqlAttribute getLeftAttribute() {
        return leftAttribute;
    }



    public PSqlAttribute getRightAttribute() {
        return rightAttribute;
    }

    @Override
    public String getStringExpression() {
        return expression.getStringExpression();
    }

    @Override
    public void accept(ParsedSqlExpressionVisitor expressionVisitor) {

    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {
    }


    @Override
    public String toString(){
        return leftAttribute.toString() + " " + expression.getStringExpression() + " " + rightAttribute;
    }
}
