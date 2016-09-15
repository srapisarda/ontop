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


import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.sql.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author  Salvatore Rapisarda on 10/07/2016.
 */
class ParsedSQLFromItemVisitor implements FromItemVisitor {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     *
     * @return  an instance of {@link ParsedSqlContext}
     */
    public ParsedSqlContext getContext() {
        return context;
    }
    private final ParsedSqlContext context;


    ParsedSQLFromItemVisitor(ParsedSqlContext context){
        this.context = context;
    }


    /**
     * Table ::-
     * database: Database,
     * schemaName: String,
     * name: String,
     * alias: Alias,
     * pivot: Pivot		#  ns
     *
     * @param table {@link Table} Object
     */
    @Override
    public void visit(Table table) {
        logger.debug("Visit Table");
        if (table.getPivot() != null)
            throw new ParseException(table.getPivot());

        RelationID name = context.getIdFac().createRelationID(table.getSchemaName(), table.getName());
        if (context.getMetadata().getRelation(name) != null) {
            DatabaseRelationDefinition databaseRelationDefinition = context.getMetadata().getDatabaseRelation(context.getIdFac().createRelationID(table.getSchemaName(), table.getName()));
            String key = table.getName();
            context.getRelations().put(context.getIdFac().createRelationID(table.getSchemaName(), key), databaseRelationDefinition);

            // Mapping table attribute
            databaseRelationDefinition.getAttributes().forEach(attribute -> {
                final QualifiedAttributeID tableQualifiedAttributeID = new QualifiedAttributeID(databaseRelationDefinition.getID(), attribute.getID());
                // quoted attribute key
                QualifiedAttributeID  keyTableAttribute = new QualifiedAttributeID(
                        null,
                        attribute.getID()
                );
                //
                context.getTableAttributes().put( keyTableAttribute,  tableQualifiedAttributeID );
            });

            context.getAttributes().putAll(context.getTableAttributes());

            if ( table.getAlias() != null && table.getAlias().getName() != null ) {
                final String aliasName = table.getAlias().getName();
                databaseRelationDefinition.getAttributes().forEach(attribute -> {
                    final QualifiedAttributeID qualifiedAttributeID = new QualifiedAttributeID(databaseRelationDefinition.getID(), attribute.getID());
                    // quoted attribute key
                    QualifiedAttributeID  keyAttribute = new QualifiedAttributeID(
                            context.getIdFac().createRelationID(null, aliasName ),
                            attribute.getID()
                    );
                    //
                    context.getAttributes().put( keyAttribute, qualifiedAttributeID );
                });
            }
        } else
            throw new MappingQueryException("table " + table.getFullyQualifiedName() + " does not exist.", table);

        logger.debug("Table alias: " + table.getAlias() + " --> " + table.getName());
    }


    @Override
    public void visit(SubSelect subSelect) {
        logger.debug(String.format("Visit SubSelect, alias -->  %1$s", subSelect.getAlias() == null ? "" : subSelect.getAlias()));

        if (subSelect.getPivot() != null)
            throw new ParseException(subSelect.getPivot());

        if (subSelect.getAlias() == null ||  subSelect.getAlias().getName().isEmpty())
            throw new MappingQueryException("A SUB_SELECT SHOULD BE IDENTIFIED BY AN ALIAS", subSelect);


        // logger.debug(String.format("select index: %3$s,  (relationLevel: %2$d):  %1$s", subSelect.toString(), relationLevel, getSelectIndex()));
            /*
            if (!(subSelect.getSelectBody() instanceof PlainSelect)) {
            throw new ParseException(subSelect);
            */

        PlainSelect subSelBody = (PlainSelect) subSelect.getSelectBody();

        // relationLevel(ParsedSqlQueryVisitor.relationIndexOperations.add);

        // only very simple subqueries are supported at the moment
            /*if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
                throw new ParseException(subSelect);*/
       // List<String> relationalMapIndex = new LinkedList<>();
        final QuotedID contextAlias =  context.getIdFac().createAttributeID( subSelect.getAlias().getName() ) ;
        ParsedSQLSelectVisitor visitor = new ParsedSQLSelectVisitor(context.getMetadata(),  contextAlias );
       //v.set( relationalMapIndex );
        subSelBody.accept(visitor);
        //context.getAttributes().putAll(visitor.getContext().getAttributes());
        context.getChildContext().put( visitor.getContext().getAlias(),  visitor.getContext());
        context.getAttributes().putAll (visitor.getContext().getTableAttributes());

        final String aliasName = subSelect.getAlias().getName();
        visitor.getContext().getTableAttributes().entrySet().forEach(attribute -> {
            // attribute attribute key
            QualifiedAttributeID  attributeKey = new QualifiedAttributeID(
                    context.getIdFac().createRelationID(null, aliasName ),
                    attribute.getKey().getAttribute()
            );
            context.getAttributes().put( attributeKey, attribute.getValue());
        });


       // v.getFromItemVisitor().getRelationMapIndex();
/*
            if (subSelBody.getWhere() != null)
                subSelBody.getWhere().accept(this);


*/
    }

    /**
     * # (to be supported later)
     * SubJoin ::-
     * left: FromItem,
     * join: Join,
     * alias: Alias,
     * pivot: Pivot		#  ns
     *
     * @param subjoin {@link SubJoin}
     */
    @Override
    public void visit(SubJoin subjoin) {
        logger.debug("Visit SubJoin");
        if (subjoin.getPivot() != null)
            throw new ParseException(subjoin.getPivot());
        // TODO: implement logic

    }

    /**
     * # LATERAL sub-select is not supported
     * LateralSubSelect ::-
     * subSelect: SubSelect,
     * alias: Alias,
     * pivot: Pivot
     *
     * @param lateralSubSelect {@link LateralSubSelect}
     */
    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        logger.debug("Visit LateralSubSelect");
        throw new ParseException(lateralSubSelect);
    }

    /**
     * ValueList ::-
     * alias: Alias,
     * multiExpressionList: MultiExpressionList,
     * noBrackets: boolean,
     * columnNames: String*
     *
     * @param valuesList {@link ValuesList}
     */
    @Override
    public void visit(ValuesList valuesList) {
        logger.debug("Visit ValuesList");

        logger.debug("ValuesList instance of " + valuesList.getClass().getName());
        // TODO:  implement logic ...  get alias
    }


}
