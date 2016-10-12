package it.unibz.inf.ontop.sql.api.ParsedSql.expressions.joins;

import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.api.ParsedSql.expressions.PSqlExpression;
import it.unibz.inf.ontop.sql.api.ParsedSql.visitors.PSqlContext;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Salvatore Rapisarda on 30/09/2016.
 *
 */
public class PSqlInnerJoinUsing implements PSqlExpression {
    private final Set<QualifiedAttributeID> commonAttributes;
    public Set<QualifiedAttributeID> getCommonAttributes() {
        return commonAttributes;
    }

    public PSqlInnerJoinUsing(PSqlContext context, List<Column> usingColumns) {
        commonAttributes = new HashSet<>();
        if ( !context.getRelations().isEmpty()  ) {
            context.getRelations().keySet().forEach(relationKey -> {
                usingColumns.forEach( column -> {
                    if (!relationKey.getSchemalessID().equals(column.getColumnName())) {
                        QualifiedAttributeID attributeKey =
                                new QualifiedAttributeID(
                                        relationKey.getSchemalessID(),
                                        context.getMetadata().getQuotedIDFactory().createAttributeID( column.getColumnName()));
                        final QualifiedAttributeID commonAttribute = context.getAttributes().get(attributeKey);
                        if (commonAttribute != null)
                            commonAttributes.add(commonAttribute);
                        else
                            throw new MappingQueryException("Unknown column in using clause.", column);
                    }
                });
            });
        }

    }


    @Override
    public void accept(PSqlExpressionVisitor expressionVisitor) {

    }

    @Override
    public void accept(ExpressionVisitor expressionVisitor) {

    }
}
