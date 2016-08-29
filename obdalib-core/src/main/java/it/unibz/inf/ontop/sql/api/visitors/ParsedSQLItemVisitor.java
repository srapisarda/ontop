package it.unibz.inf.ontop.sql.api.visitors;

import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.sql.DBMetadata;
import it.unibz.inf.ontop.sql.QualifiedAttributeID;
import it.unibz.inf.ontop.sql.QuotedID;
import it.unibz.inf.ontop.sql.RelationID;
import it.unibz.inf.ontop.sql.api.ParsedSqlContext;
import it.unibz.inf.ontop.sql.api.ParsedSqlPair;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Salvatore Rapisarda on 20/07/2016.
 */
class ParsedSQLItemVisitor implements SelectItemVisitor {
    private Logger logger = LoggerFactory.getLogger(this.getClass());



    private final RelationID relationID;

    /**
     *
     * @return  an instance of {@link ParsedSqlContext}
     */
    public ParsedSqlContext getContext() {
        return context;
    }
    private final ParsedSqlContext context;

    ParsedSQLItemVisitor(DBMetadata metadata, RelationID relationID){
        context= new ParsedSqlContext(metadata);
        this.relationID = relationID;
    }

    @Override
    public void visit(AllColumns allColumns) {
        logger.debug("visit AllColumns");

    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        logger.debug("visit allTableColumns");
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        logger.debug("visit selectExpressionItem");

        // TODO:  should support complex expressions
        final ParsedSQLExpressionVisitor parsedSQLExpressionVisitor = new ParsedSQLExpressionVisitor();
        selectExpressionItem.getExpression().accept(parsedSQLExpressionVisitor);
        parsedSQLExpressionVisitor
                .getColumns()
                .forEach(column -> addAttributeAliasMap(column.getColumnName(),
                        selectExpressionItem.getAlias() == null? column.getColumnName() : selectExpressionItem.getAlias().getName().toString(),
                        context.getIdFac().createRelationID(null,
                                column.getTable().getAlias() != null ? column.getTable().getAlias().getName() : column.getTable().getName())));

    }


    private void addAttributeAliasMap(String attributeId, String alias, RelationID relationID) {
        QuotedID quotedID = context.getIdFac().createAttributeID(attributeId);
        QuotedID quotedIdAlias  =  alias == null ?
                quotedID : context.getIdFac().createAttributeID(alias);

        ParsedSqlPair<ImmutableList<RelationID>,QualifiedAttributeID> pair = new ParsedSqlPair<>(
                ImmutableList.of ((relationID == null || relationID.getTableName() == null) ? context.getIdFac().createRelationID(null, ""): relationID ),
                new QualifiedAttributeID(relationID, quotedIdAlias));

        context.getAttributeAliasMap().put(pair, quotedID);
    }
}
