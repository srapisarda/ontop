package it.unibz.inf.ontop.sql.api.visitors;

/*
 * #%L
 * ontop-obdalib-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.ImmutableList;
import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.sql.*;
import it.unibz.inf.ontop.sql.api.ParsedSqlContext;
import it.unibz.inf.ontop.sql.api.ParsedSqlPair;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;


/**
 * @author Salvatore Rapisarda on 20/07/2016.
 */
class ParsedSQLItemVisitor implements SelectItemVisitor {
    private Logger logger = LoggerFactory.getLogger(this.getClass());



    private final Map<RelationID, DatabaseRelationDefinition> relations;

    /**
     *
     * @return  an instance of {@link ParsedSqlContext}
     */
    public ParsedSqlContext getContext() {
        return context;
    }
    private final ParsedSqlContext context;

    ParsedSQLItemVisitor(DBMetadata metadata, Map<RelationID, DatabaseRelationDefinition> relations){
        context= new ParsedSqlContext(metadata);
        this.relations = relations;
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
                .forEach(column -> addAttributeAliasMap(
                        column.getColumnName(),
                        selectExpressionItem.getAlias() == null? column.getColumnName() : selectExpressionItem.getAlias().getName().toString(),
                        context.getIdFac().createRelationID(null, column.getTable().getAlias() != null ? column.getTable().getAlias().getName() : column.getTable().getName())));

    }


    private void addAttributeAliasMap(String attributeId, String alias, RelationID relationID) {
        QuotedID quotedID = context.getIdFac().createAttributeID(attributeId);
        QuotedID quotedIdAlias  =  alias == null ?
                quotedID : context.getIdFac().createAttributeID(alias);

        if( relationID.getTableName() == null   ) {
            final Optional<Map.Entry<RelationID, DatabaseRelationDefinition>> first =
                    relations.entrySet()
                            .stream()
                            .filter(p -> p.getValue().getAttributes().stream()
                                    .anyMatch(q ->
                                            q.getID().getName().toLowerCase().equals(quotedID.getName().toLowerCase()))).findFirst();
            if (first.isPresent() )
                relationID =  first.get().getKey();
            else
                throw new MappingQueryException( "the attribute is not present in any relation.", attributeId  );

        }

        final QualifiedAttributeID qualifiedAttributeID = new QualifiedAttributeID(relationID, quotedIdAlias);
        ParsedSqlPair<ImmutableList<RelationID>,QualifiedAttributeID> listPair =
                new ParsedSqlPair<>( ImmutableList.of (relationID), qualifiedAttributeID );

        context.getProjectedAttributes().put(  new ParsedSqlPair<>( relationID, qualifiedAttributeID ), quotedID);

    }

}
