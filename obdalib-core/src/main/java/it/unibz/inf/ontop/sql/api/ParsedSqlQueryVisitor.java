package it.unibz.inf.ontop.sql.api;

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
import it.unibz.inf.ontop.sql.DBMetadata;
import it.unibz.inf.ontop.sql.QuotedIDFactory;
import it.unibz.inf.ontop.sql.RelationID;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A structure to store the parsed SQL query string. It returns the information
 * about the query using the visitor classes
 */
public class ParsedSqlQueryVisitor  {
    private  final Logger logger =  LoggerFactory.getLogger(getClass());

    // fromItemVisitor in inner class

    private final QuotedIDFactory idFac;
    private final DBMetadata metadata;
    private final short startingIndex = 0;
    private long relationScope = startingIndex;
    private long relationLevel = startingIndex;

    private final Set<RelationID> tables;
    public Set<RelationID> getTables() {
        return tables;
    }



    private  Map<String, Map<String, RelationID>> relationsMap;
    public Map<String, Map<String, RelationID>>  getRelationsMap(){
        return  relationsMap;
    }

    /**
     *  This constructor get in input the instance of
     *  {@link Select} and  {@link DBMetadata} object
     *
     * @param selectQuery {@link Select}
     * @param metadata {@link DBMetadata}
     */
    public ParsedSqlQueryVisitor(Select selectQuery, DBMetadata metadata){
        this.metadata = metadata;
        this.idFac = metadata.getQuotedIDFactory();
        this.tables = new HashSet<>();
        this.relationsMap = new HashMap<>();

        // WITH operations are not supported
        if (selectQuery.getWithItemsList() != null && ! selectQuery.getWithItemsList().isEmpty())
            throw new ParseException(selectQuery.getWithItemsList());

        selectQuery.getSelectBody().accept(selectVisitor);
    }

    /**
     * The only operation sported for this visitor is the PlainSelect
     *
     * This the grammar for this visitor
     * SelectBody ::=
     *                   PlainSelect
     *                  | SetOperationList  	# ns
     *                  | WithItem 		        # ns
     *    "ns" means not supported.
     *
     *
     * PlainSelect ::-
     *                  selectItems: SelectItem*,
     *                  fromTable: FromItem,
     *                  join: Join*,
     *                  where: Expression,
     *                  distinct: Distinct, 	# ns
     *                  having: Expression, 	# ns
     *                  groupByColumnReferences: Expression*, 	# ns
     *                  queryLevelByElements : OrderByElement*, 	# ns
     *                  limit: Limit, 		# ns
     *                  top: Top, 		# ns
     *                  oracleHierarchical: OracleHierarchicalExpression, 	# ns
     *                  intoTables: Table* 	# FAIL
     */
    private SelectVisitor selectVisitor = new SelectVisitor() {
        @Override
        public void visit(PlainSelect plainSelect) {
            logger.info("Visit PlainSelect");

            if (plainSelect.getDistinct() != null)
                throw new ParseException(plainSelect.getDistinct());
            if (plainSelect.getHaving() != null)
                throw new ParseException(plainSelect.getHaving());
            if (plainSelect.getGroupByColumnReferences() != null && ! plainSelect.getGroupByColumnReferences().isEmpty() )
                throw new ParseException(plainSelect.getGroupByColumnReferences());
            if (plainSelect.getOrderByElements() != null && ! plainSelect.getOrderByElements().isEmpty() )
                throw new ParseException(plainSelect.getOrderByElements());
            if (plainSelect.getLimit() != null)
                throw new ParseException(plainSelect.getLimit());
            if (plainSelect.getLimit() != null)
                throw new ParseException(plainSelect.getLimit());
            if (plainSelect.getTop()!= null)
                throw new ParseException(plainSelect.getTop());
            if (plainSelect.getOracleHierarchical() != null  )
                throw new ParseException(plainSelect.getOracleHierarchical() );
            if (plainSelect.getIntoTables() != null && ! plainSelect.getIntoTables().isEmpty()  )
                throw new MappingQueryException("INTO TABLE IS NOT ALLOWED!!! FAIL!", plainSelect.getIntoTables() );

            logger.info( String.format( "PlainSelect (relationLevel: %2$d):  %1$s",  plainSelect.toString(), relationLevel) );

            plainSelect.getFromItem().accept(fromItemVisitor);
            if (plainSelect.getJoins() != null) {
                plainSelect.getJoins().forEach( join -> join.getRightItem().accept(fromItemVisitor) );
            }
            checkRelationScope();
            relationLevel(relationIndexOperations.reset);
//        if (subSelBody.getWhere() != null)
//            subSelBody.getWhere().accept(this);
        }



        /**
         * This is a not supported method. It throw a {@link ParseException}
         * @param setOpList {@link SetOperationList}
         */
        @Override
        public void visit(SetOperationList setOpList) {
            logger.info("Visit SetOperationList");
            throw new ParseException(setOpList);
        }

        /**
         * This is a not supported method. It throw a {@link ParseException}
         * @param withItem {@link WithItem}
         */
        @Override
        public void visit(WithItem withItem) {
            logger.info("Visit WithItem");
            throw new ParseException(withItem);
        }
    };


    /**
     * This class implement a {@link FromItemVisitor}
     */
    private FromItemVisitor fromItemVisitor =    new FromItemVisitor() {

        /**
         * Table ::-
         *          database: Database,
         *          schemaName: String,
         *          name: String,
         *          alias: Alias,
         *          pivot: Pivot		#  ns
         * @param table {@link Table} Object
         */
        @Override
        public void visit(Table table) {
            logger.info("Visit Table");
            if (table.getPivot() != null  )
                throw new ParseException(table.getPivot() );



            logger.info( String.format( "table (relationLevel: %2$d):  %1$s",  table.toString(), relationLevel) );
            RelationID name =  RelationID.createRelationIdFromDatabaseRecord ( idFac,  table.getSchemaName(),  table.getName() );
            if ( metadata.getRelation( name ) != null ) {
                tables.add(name);
                addRelationToMap(table.getAlias().toString(), name);
            }else
                throw new MappingQueryException("the table " + table.getFullyQualifiedName() + " does not exist.", table);
            logger.info( "Table alias: " + table.getAlias() + " --> " + table.getName());



        }



        @Override
        public void visit(SubSelect subSelect) {
            logger.info("Visit SubSelect");

            relationLevel(relationIndexOperations.add);

            if (subSelect.getPivot() != null )
                throw new ParseException(subSelect.getPivot());
            logger.info( String.format( "select index: %3$s,  (relationLevel: %2$d):  %1$s",  subSelect.toString(), relationLevel , getSelectIndex() ) );
            /* if (!(subSelect.getSelectBody() instanceof PlainSelect)) {
            throw new ParseException(subSelect);
            */

            PlainSelect subSelBody = (PlainSelect) subSelect.getSelectBody();

            // only very simple subqueries are supported at the moment
            /*if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
                throw new ParseException(subSelect);*/
            subSelBody.accept(selectVisitor);

/*
            if (subSelBody.getWhere() != null)
                subSelBody.getWhere().accept(this);


*/
        }

        /**
         * # (to be supported later)
         * SubJoin ::-
         *              left: FromItem,
         *              join: Join,
         *              alias: Alias,
         *              pivot: Pivot		#  ns
         * @param subjoin {@link SubJoin}
         */
        @Override
        public void visit(SubJoin subjoin) {
            logger.info("Visit SubJoin");
            if (subjoin.getPivot() != null )
                throw new ParseException(subjoin.getPivot());
            // TODO: implement logic

        }

        /**
         * # LATERAL sub-select is not supported
         *  LateralSubSelect ::-
         *                      subSelect: SubSelect,
         *                      alias: Alias,
         *                      pivot: Pivot
         * @param lateralSubSelect {@link LateralSubSelect}
         */
        @Override
        public void visit(LateralSubSelect lateralSubSelect) {
            logger.info("Visit LateralSubSelect");
            throw new ParseException(lateralSubSelect);
        }

        /**
         * ValueList ::-
         *              alias: Alias,
         *              multiExpressionList: MultiExpressionList,
         *              noBrackets: boolean,
         *              columnNames: String*
         * @param valuesList {@link ValuesList}
         */
        @Override
        public void visit(ValuesList valuesList) {
            logger.info("Visit ValuesList");

            logger.info("ValuesList instance of " + valuesList.getClass().getName());
            // TODO:  implement logic ...  get alias
        }

    };

    private String getSelectIndex(){
        return relationScope + "."  + relationLevel;
    }

    private void checkRelationScope() {
        if ( relationLevel > 0 ){
            relationScope(relationIndexOperations.add);
        }
    }

    private void relationLevel(relationIndexOperations op ){
        switch ( op ) {
            case reset:
                relationLevel = startingIndex;
                break;
            case add:
                relationLevel++;
                break;
        }
    }
    private void relationScope(relationIndexOperations op ){
        switch ( op ) {
            case reset:
                relationScope = startingIndex;
                break;
            case add:
                relationScope++;
                break;
        }
    }
    private enum relationIndexOperations { reset, add }

    private void addRelationToMap(String alias,  RelationID name) {
        String key =getSelectIndex();
        if (!relationsMap.containsKey(key)){
            relationsMap.put(key, new HashMap<>() );
        }
        relationsMap.get(key).put( alias==null ? name.getTableName() : alias.trim(), name);

    }


}
