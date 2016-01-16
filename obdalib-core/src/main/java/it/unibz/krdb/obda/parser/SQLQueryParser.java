package it.unibz.krdb.obda.parser;

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


import it.unibz.krdb.obda.parser.exception.MappingQueryException;
import it.unibz.krdb.obda.parser.exception.ParseException;
import it.unibz.krdb.obda.parser.visitor.JoinExpressionVisitor;
import it.unibz.krdb.obda.parser.visitor.ProjectorExpressionVisitor;
import it.unibz.krdb.obda.parser.visitor.TableExpressionVisitor;
import it.unibz.krdb.obda.parser.visitor.WhereExpressionVisitor;
import it.unibz.krdb.sql.*;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
    This visitor is used to parse SQL amd check the OBDA query compatibility
 */
public class SQLQueryParser {

    //region Private Variables
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final DBMetadata dbMetadata;
    private final QuotedIDFactory idFac;

    private final Map<RelationID, RelationID> tableAlias = new HashMap<>();
    private final Map<QuotedID, Expression> expressionAlias = new HashMap<>();
    private final List<Expression> joinConditions = new LinkedList<>();

    // maps attribute names to attributes
    // important: it maps both
    //       (a) fully qualified names that include relation names or aliases (if present)
    //       (b) unqualified names that can be unambiguously resolved
    private final Map<QualifiedAttributeID, Attribute> fromAttributesIds = new HashMap<>();

    private Expression whereClause;
    private final List<SelectItem> projection = new LinkedList<>();

    public List<RelationID> getRelations() {
        return relations;
    }

    private final List<RelationID> relations = new LinkedList<>();

    private  List<Column>  joinUsingColumns = null;

    private final ExpressionVisitor tableExpressionVisitor;
    private final ExpressionVisitor projectorExpressionVisitor;
    private final ExpressionVisitor joinExpressionVisitor;
    private final ExpressionVisitor whereExpressionVisitor;
    //endregion

    public SQLQueryParser(Select selectQuery, DBMetadata dbMetadata) {
        this.dbMetadata = dbMetadata;
        this.idFac =  this.dbMetadata.getQuotedIDFactory();

        tableExpressionVisitor = new TableExpressionVisitor(itemsListVisitor);
        projectorExpressionVisitor = new ProjectorExpressionVisitor(idFac);
        joinExpressionVisitor = new JoinExpressionVisitor(idFac, joinConditions);
        whereExpressionVisitor = new WhereExpressionVisitor(idFac);

        // the WITH is not supported
        if (selectQuery.getWithItemsList() != null)
            throw new ParseException(selectQuery.getWithItemsList());

        // CATCH ParseException
        selectQuery.getSelectBody().accept(selectVisitor);

    }


    //region Properties
    /**
     *  All found tableAlias is returned as a Map of RelationID @see it.unibz.krdb.sql.RelationID
     *  @return Map of RelationID
     */
    public Map<RelationID, RelationID> getTableAlias() {
        return tableAlias;
    }

    /**
     * Return a map between the column in the select statement and its alias.
     * @return alias map
    */
    public Map<QuotedID, Expression> getExpressionAlias() {
        return expressionAlias;
    }

    /**
     * Obtain the join conditions in a format "expression condition expression"
     * for example "table1.home = table2.house"<br>
     *
     * BRINGS TABLE NAME / SCHEMA / ALIAS AND COLUMN NAMES in the JOIN / JOIN ON clauses into NORMAL FORM
     *
     * @return a list of Expression containing the join conditions
     */
    public List<Expression> getJoinConditions() {
        return joinConditions;
    }

    /**
     * Give the WHERE clause of the SELECT statement<br>
     *
     * NOTE: is also BRINGS ALL SCHEMA / TABLE / ALIAS / COLUMN NAMES in the WHERE clause into NORMAL FORM
     *
     * @return an Expression represent a where clause
     */
    public Expression getWhereClause() {
        return whereClause;
    }

    /**
     * Return the list of Projection with the expressions between SELECT and FROM<br>
     *
     * @return projection object
     */
    public List<SelectItem> getProjection() {
        return projection;
    }



    //endregion


    private void visitSubSelect(SubSelect subSelect) {
        if (!(subSelect.getSelectBody() instanceof PlainSelect))
            throw new ParseException(subSelect);

        PlainSelect subSelBody = (PlainSelect)subSelect.getSelectBody();

        // only very simple subqueries are supported at the moment
        if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
            throw new ParseException(subSelect);

        subSelBody.accept(selectVisitor);
    }

    /**
     *
     * @param idFac QuotedIDFactory object
     * @param tableColumn query columns
     */
    public static void normalizeColumnName(QuotedIDFactory idFac, Column tableColumn) {
        QuotedID columnName = idFac.createAttributeID(tableColumn.getColumnName());
        tableColumn.setColumnName(columnName.getSQLRendering());

        Table table = tableColumn.getTable();
        RelationID tableName = idFac.createRelationID(table.getSchemaName(), table.getName());
        table.setSchemaName(tableName.getSchemaSQLRendering());
        table.setName(tableName.getTableNameSQLRendering());
    }


    private final SelectVisitor selectVisitor = new SelectVisitor() {
        @Override
        public void visit(PlainSelect plainSelect) {
            // the SELECT should correspond to a CQ
            validatePlainSelect(plainSelect);

            // projection
            for (SelectItem item : plainSelect.getSelectItems())
                item.accept(selectItemVisitor); // todo: review

            // from items
            processFROM(plainSelect);

            if (plainSelect.getWhere() != null) {
                whereClause = plainSelect.getWhere();
                //we visit the where clause to fix any and all comparison
                whereClause.accept(whereExpressionVisitor);
                whereClause.accept(tableExpressionVisitor);
            }

        }
        @Override
        public void visit(SetOperationList setOpList) {
            throw new ParseException(setOpList);
        }

        @Override
        public void visit(WithItem withItem) {
            throw new ParseException(withItem);
        }

        /**
         * This validates that a PlainSelect Object is datalog suitable
         *
         * @param plainSelect Plain select query
         */
        private void validatePlainSelect(PlainSelect plainSelect) {
            if (plainSelect.getDistinct() != null)
                throw new ParseException(plainSelect.getDistinct());

            if (plainSelect.getIntoTables() != null) // ERROR in the mapping
                throw new MappingQueryException("Mappings cannot contain SELECT INTO", plainSelect.getIntoTables());

            if (plainSelect.getHaving() != null)
                throw new ParseException(plainSelect.getHaving());

            if (plainSelect.getGroupByColumnReferences() != null)
                throw new ParseException(plainSelect.getGroupByColumnReferences());

            if (plainSelect.getOrderByElements() != null)
                throw new ParseException(plainSelect.getOrderByElements());

            if (plainSelect.getLimit() != null)
                throw new ParseException(plainSelect.getLimit());

            if (plainSelect.getTop() != null)
                throw new ParseException(plainSelect.getTop());

            if (plainSelect.getOracleHierarchical() != null)
                throw new ParseException(plainSelect.getOracleHierarchical());

            if (plainSelect.isOracleSiblings())
                throw new ParseException(plainSelect.isOracleSiblings());
        }


        private void processFROM(PlainSelect plainSelect) {

            plainSelect.getFromItem().accept(fromItemVisitor);

            List<Join> joins = plainSelect.getJoins();
            if (joins == null)
                return;

            for (Join join : joins) {
                if (join.isOuter() || join.isLeft() || join.isRight() ) {
                    throw new ParseException(join);
                }else if ( join.isNatural() ){
                    join.getRightItem().accept(fromNaturalJoinItemVisitor);
                } else if( join.isCross() || join.isSimple() ){
                    join.getRightItem().accept(fromItemVisitor);
                } else if (join.getOnExpression() != null) {
                    join.getRightItem().accept(fromItemVisitor);
                    // ROMAN: careful with the joinExpressionVisitor - requires careful revision
                    join.getOnExpression().accept(joinExpressionVisitor);
                } else if ( join.getUsingColumns() != null) {
                    joinUsingColumns =  join.getUsingColumns();
                    join.getRightItem().accept(fromUsingColumnJoinItemVisitor);
                    joinUsingColumns = null;
                    join.getRightItem().accept(fromItemVisitor);
                }
            }
        }
    };


    private final SelectItemVisitor selectItemVisitor = new SelectItemVisitor() {

        /**
         * SELECT *
         */
    	@Override
        public void visit(AllColumns allColumns) {
            projection.add(allColumns);
        }

        /**
         * SELECT table.*
         */
        @Override
        public void visit(AllTableColumns allTableColumns) {
            projection.add(allTableColumns);
        }

        /**
         * Add the projection for the selectExpressionItem
         */
        @Override
        public void visit(SelectExpressionItem selectExpr) {
            // projection
        	Expression expr = selectExpr.getExpression();
            projection.add(selectExpr);

            if ( expr instanceof SubSelect ){
                visitSubSelect((SubSelect) expr );
            }

            expr.accept(projectorExpressionVisitor);


            Alias alias = selectExpr.getAlias();
            // all complex expressions in SELECT must be named (by aliases)
            if (!(expr instanceof Column) && alias == null)
                throw new ParseException(selectExpr);

            // alias
            if (alias  != null) {
                // NORMALIZE EXPRESSION ALIAS NAME (ROMAN 9 Dec 2015: this should be done later; the visitor should not modify the query)
                QuotedID aliasName = idFac.createAttributeID(alias.getName());
                alias.setName(aliasName.getSQLRendering());
                expressionAlias.put(aliasName, expr);
            }


        }
    };

    private Map.Entry<RelationID, RelationDefinition> getTableDefinitionWithAlias(FromItem fromItem) {
        if (!(fromItem instanceof  Table))
            throw new ParseException(fromItem);

        Table table = (Table) fromItem;

        RelationID relationId = idFac.createRelationID(table.getSchemaName(), table.getName());
        RelationDefinition relation = dbMetadata.getRelation(relationId);
        if (relation  == null )
            throw new MappingQueryException("Relation does not exist", relationId);

        relations.add(relationId);

        Alias as = table.getAlias();
        RelationID aliasId = (as != null) ? idFac.createRelationID(null, as.getName()) : relationId;
        if (tableAlias.containsKey(aliasId))
            throw new MappingQueryException("Ambiguous alias", aliasId);

        tableAlias.put(aliasId, relationId);
        return new AbstractMap.SimpleImmutableEntry<>(aliasId, relation);
    }


    private final FromItemVisitor fromUsingColumnJoinItemVisitor = new FromItemVisitor() {

        /**
         * this is used to visit join using columns
         * @param table join
         **/
       @Override
        public void visit(Table table) {

            RelationID relationId = idFac.createRelationID(table.getSchemaName(), table.getName());
            RelationDefinition relation = dbMetadata.getRelation(relationId);
            if (relation == null)
                throw new MappingQueryException("Relation does not exist", relationId);

            Map<QualifiedAttributeID, Attribute> rightAttributes = new HashMap<>();
            for (Attribute attribute : relation.getAttributes())
                rightAttributes.put(attribute.getQualifiedID(), attribute);

            for (Column column : joinUsingColumns ) {
                QuotedID attributeID = idFac.createAttributeID(column.getColumnName());
                QualifiedAttributeID rightColumnId = new QualifiedAttributeID(relationId, attributeID);
                QualifiedAttributeID leftColumnId = new QualifiedAttributeID(null, attributeID);
                if (fromAttributesIds.containsKey(leftColumnId) && rightAttributes.containsKey(rightColumnId)) {
                    Attribute rightAttribute = rightAttributes.get(rightColumnId);
                    Attribute leftAttribute = fromAttributesIds.get(leftColumnId);
                    if (leftAttribute == null)
                        throw new MappingQueryException("Ambiguous attribute", leftAttribute); // ambiguity

                    addNewBinaryJoinCondition(leftAttribute, rightAttribute, leftAttribute.getID().getName(), new EqualsTo());
                } else
                    throw new MappingQueryException("Ambiguous attribute", rightAttributes);
            }

        }

        @Override
        public void visit(SubSelect subSelect) {
            // todo : for now it rises an exception but this should be implemented for query such as: SELECT * FROM ...
            throw new ParseException(subSelect);
        }

        @Override
        public void visit(SubJoin subjoin) {
            throw new ParseException(subjoin);
        }

        @Override
        public void visit(LateralSubSelect lateralSubSelect) {
            throw new ParseException(lateralSubSelect);
        }

        @Override
        public void visit(ValuesList valuesList) {
            throw new ParseException(valuesList);
        }
    };

    private final FromItemVisitor fromNaturalJoinItemVisitor = new FromItemVisitor() {

        @Override
        public void visit(Table table) {
            Map.Entry<RelationID, RelationDefinition> rightRd = getTableDefinitionWithAlias(table);
            for (Attribute rightAttribute :  rightRd.getValue().getAttributes()) {
                QualifiedAttributeID shortId = new QualifiedAttributeID(null, rightAttribute.getID());
                if (fromAttributesIds.containsKey(shortId)) {
                    Attribute leftAttribute = fromAttributesIds.get(shortId);
                    if (leftAttribute == null)
                        throw new MappingQueryException("Ambiguous attribute", shortId); // ambiguity

                    // this attribute is shared -- add a join condition
                    addNewBinaryJoinCondition(leftAttribute, rightAttribute, leftAttribute.getID().getName(), new EqualsTo());
                }
                else {
                    // this attribute is not shared -- add to the list instead
                    RelationID aliasId = rightRd.getKey();
                    QualifiedAttributeID id = new QualifiedAttributeID(aliasId, rightAttribute.getID());
                    fromAttributesIds.put(id, rightAttribute);
                    fromAttributesIds.put(shortId, rightAttribute); // add an unqualified version (unambiguous)
                }
            }
        }

        @Override
        public void visit(SubSelect subSelect) {
            // TODO: the code from visitSubSelect should be copied here
            visitSubSelect(subSelect);
        }

        @Override
        public void visit(SubJoin subjoin) {
            throw new ParseException(subjoin);
        }

        @Override
        public void visit(LateralSubSelect lateralSubSelect) {
            throw new ParseException(lateralSubSelect);
        }

        @Override
        public void visit(ValuesList valuesList) {
            throw new ParseException(valuesList);
        }
    };

    private final FromItemVisitor fromItemVisitor = new FromItemVisitor() {

		@Override
        public void visit(Table table) {
            Map.Entry<RelationID, RelationDefinition> e = getTableDefinitionWithAlias(table);
            RelationID aliasId = e.getKey();
            for (Attribute att : e.getValue().getAttributes()) {
                QualifiedAttributeID id = new QualifiedAttributeID(aliasId, att.getID());
                fromAttributesIds.put(id, att);
                // short name, without any table name or alias
                QualifiedAttributeID shortId = new QualifiedAttributeID(null, att.getID());
                if (!fromAttributesIds.containsKey(shortId))
                    fromAttributesIds.put(shortId, att); // add an unqualified version (unambiguous)
                else
                    fromAttributesIds.put(shortId, null); // ambiguous - replace with the null marker
            }
        }

        @Override
        public void visit(SubSelect subSelect) {
            // TODO: the code from visitSubSelect should be copied here
            visitSubSelect(subSelect);
        }

        @Override
        public void visit(SubJoin subjoin) {
            throw new ParseException(subjoin);
        }

        @Override
        public void visit(LateralSubSelect lateralSubSelect) {
            throw new ParseException(lateralSubSelect);
        }

        @Override
        public void visit(ValuesList valuesList) {
            throw new ParseException(valuesList);
        }
    };

    //
    private final ItemsListVisitor itemsListVisitor = new ItemsListVisitor() {
        @Override
        public void visit(ExpressionList expressionList) {
            for (Expression expression : expressionList.getExpressions())
                expression.accept(tableExpressionVisitor);
        }

        @Override
        public void visit(MultiExpressionList multiExprList) {
            throw new ParseException(multiExprList);
            //for (ExpressionList exprList : multiExprList.getExprList())
            //    exprList.accept(this);
        }

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }
    };



    /**
     * Adds a new binary expression to the joinExpressionVisitor list
     * @param leftAttribute - LHS binary attribute
     * @param rightAttribute - RHS binary attribute
     * @param columnName - common column name between LHS and RHS
     * @param binaryExpression - Binary expression or one of its extensions
     */
    private void addNewBinaryJoinCondition( Attribute leftAttribute, Attribute rightAttribute, String columnName,  BinaryExpression binaryExpression){
        Column leftColumn = new Column(
                new Table( leftAttribute.getRelation().getID().getSchemaName(),
                        leftAttribute.getRelation().getID().getTableName()), columnName );

        Column rightColumn = new Column(
                new Table(  rightAttribute.getRelation().getID().getSchemaName(),
                        rightAttribute.getRelation().getID().getTableName()), columnName);
        rightColumn.accept(joinExpressionVisitor);

        binaryExpression.setLeftExpression(leftColumn);
        binaryExpression.setRightExpression(rightColumn);
        if (!joinConditions.contains(binaryExpression)) {
            joinConditions.add(binaryExpression);
        }
    }
    //endregion

}
