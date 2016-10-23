package it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins;

import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlExpression;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlContext;
import net.sf.jsqlparser.expression.ExpressionVisitor;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by salvo on 17/09/2016.
 *
 */
public class PSqlNaturalJoin implements PSqlExpression {

    private final Set<QualifiedAttributeID> commonAttributes;

    public PSqlNaturalJoin(PSqlContext context) {
        commonAttributes = new HashSet<>();
        context.getAttributes().keySet().forEach( attribute -> {
            if ( attribute.getRelation() != null ) {
                context.getRelations().keySet().forEach(relationKey -> {
                    if (!relationKey.getSchemalessID().equals(attribute.getRelation())) {
                        QualifiedAttributeID attributeKey = new QualifiedAttributeID(relationKey.getSchemalessID(), attribute.getAttribute());
                        final QualifiedAttributeID commonAttribute = context.getAttributes().get(attributeKey);
                        if (commonAttribute != null)
                            commonAttributes.add(commonAttribute);
                    }
                });
            }
        });

    }

    @Override
    public String toString() {
        return "natural join";
    }

    public Set<QualifiedAttributeID> getCommonAttributes() {
        return commonAttributes;
    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {

    }

    @Override
    public void accept(ParsedSqlExpressionVisitor expressionVisitor) {

    }
}
