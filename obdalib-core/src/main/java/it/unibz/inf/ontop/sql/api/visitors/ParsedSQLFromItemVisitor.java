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
import com.sun.tools.javac.util.Pair;
import it.unibz.inf.ontop.exception.MappingQueryException;
import it.unibz.inf.ontop.exception.ParseException;
import it.unibz.inf.ontop.sql.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author  Salvatore Rapisarda on 10/07/2016.
 */
class ParsedSQLFromItemVisitor implements FromItemVisitor {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private QuotedIDFactory idFac;
    private DBMetadata metadata;
    private Set<RelationID> tables = new HashSet<>();

    Map<ImmutableList<RelationID>, DatabaseRelationDefinition> getRelationAliasMap() {
        return relationAliasMap;
    }


    private Map<ImmutableList<RelationID>, DatabaseRelationDefinition> relationAliasMap;

    private final Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID >, QuotedID> attributeAliasMap;
    Map<Pair<ImmutableList<RelationID>, QualifiedAttributeID>, QuotedID> getAttributeAliasMap() {
        return attributeAliasMap;
    }


    public Set<RelationID> getTables() {
        return tables;
    }


    ParsedSQLFromItemVisitor(DBMetadata metadata){
        this.metadata = metadata;
        this.idFac = metadata.getQuotedIDFactory();
        this.relationAliasMap = new LinkedHashMap<>();
        this.attributeAliasMap = new LinkedHashMap<>();
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
        logger.info("Visit Table");
        if (table.getPivot() != null)
            throw new ParseException(table.getPivot());

        RelationID name = RelationID.createRelationIdFromDatabaseRecord(idFac, table.getSchemaName(), table.getName());
        if (metadata.getRelation(name) != null) {
            tables.add(name);
            String key = (table.getAlias() != null ? table.getAlias().getName() : table.getName());
            this.relationAliasMap.put(
                    ImmutableList.<RelationID>builder().add(RelationID.createRelationIdFromDatabaseRecord(this.idFac, null, key)).build(),
                    metadata.getDatabaseRelation(RelationID.createRelationIdFromDatabaseRecord(idFac, table.getSchemaName(), table.getName())));

        } else
            throw new MappingQueryException("the table " + table.getFullyQualifiedName() + " does not exist.", table);

        logger.info("Table alias: " + table.getAlias() + " --> " + table.getName());

    }


    @Override
    public void visit(SubSelect subSelect) {
        logger.info(String.format("Visit SubSelect, alias -->  %1$s", subSelect.getAlias() == null ? "" : subSelect.getAlias()));

        if (subSelect.getPivot() != null)
            throw new ParseException(subSelect.getPivot());

        if (subSelect.getAlias() == null ||  subSelect.getAlias().getName().isEmpty())
            throw new MappingQueryException("A SUB_SELECT SHOULD BE IDENTIFIED BY AN ALIAS!!!", subSelect);

        // logger.info(String.format("select index: %3$s,  (relationLevel: %2$d):  %1$s", subSelect.toString(), relationLevel, getSelectIndex()));
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

        ParsedSQLSelectVisitor visitor = new ParsedSQLSelectVisitor(metadata);
       //v.set( relationalMapIndex );
        subSelBody.accept(visitor);
        this.tables.addAll(visitor.getTables());

        final String alias =  subSelect.getAlias().getName();



        visitor.getRelationAliasMap().forEach(( k, v ) -> {
            final ImmutableList.Builder<RelationID> builder = ImmutableList.<RelationID>builder().add(RelationID.createRelationIdFromDatabaseRecord(this.idFac, null, alias));
            k.forEach(builder::add);
            this.relationAliasMap.put(builder.build(), v);
        });


        visitor.getAttributeAliasMap().forEach( (k, v ) -> {
            final Optional<ImmutableList<RelationID>> relationIDsOptional = this.relationAliasMap.keySet().stream()
                    .filter(p ->
                            p.stream().anyMatch(q ->
                                    q.getTableName() != null &&
                                            q.getTableName().equals(alias))).findAny();


            if ( relationIDsOptional.isPresent()){
                final ImmutableList<RelationID> immutableListRelations = relationIDsOptional.get();
                final ImmutableList.Builder<RelationID> builder = ImmutableList.builder();//.add(RelationID.createRelationIdFromDatabaseRecord(this.idFac, null, alias));
                builder.addAll( immutableListRelations);
                this.getAttributeAliasMap().put( new Pair<>(builder.build(), k.snd ), v);
            }else
                throw new MappingQueryException("the relationAliasMap does not contains any alias ",relationAliasMap ); // cannot append

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
        logger.info("Visit SubJoin");
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
        logger.info("Visit LateralSubSelect");
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
        logger.info("Visit ValuesList");

        logger.info("ValuesList instance of " + valuesList.getClass().getName());
        // TODO:  implement logic ...  get alias
    }


}
