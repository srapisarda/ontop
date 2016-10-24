package it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins;

import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlAttribute;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlCondition;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlExpression;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlContext;
import net.sf.jsqlparser.expression.ExpressionVisitor;

import java.util.*;

/**
 * Created by salvo on 17/09/2016.
 *
 */
public class PSqlNaturalJoin implements PSqlExpression {

    private final Set<QualifiedAttributeID> commonAttributes;

    public PSqlNaturalJoin(PSqlContext context) {
        commonAttributes = new HashSet<>();
        final int add[] = {0};
        final QualifiedAttributeID[] prev = new QualifiedAttributeID[1];
        context.getAttributes().keySet().forEach( attribute -> {
            if ( attribute.getRelation() != null ) {
                context.getRelations().keySet().forEach(relationKey -> {
                    if (!relationKey.getSchemalessID().equals(attribute.getRelation())) {
                        QualifiedAttributeID attributeKey = new QualifiedAttributeID(relationKey.getSchemalessID(), attribute.getAttribute());
                        final QualifiedAttributeID commonAttribute = context.getAttributes().get(attributeKey);
                        if (commonAttribute != null) {
                            commonAttributes.add(commonAttribute);
                            if (add[0]==1){
                                add[0]=0;
                                final PSqlCondition condition = new PSqlCondition(commonAttribute, prev[0]);
                                context.getConditions().add(condition);
                            }
                            prev[0] = commonAttribute;
                            add[0]++;
                        }
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
