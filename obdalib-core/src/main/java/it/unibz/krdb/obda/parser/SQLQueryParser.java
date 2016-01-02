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


import it.unibz.krdb.sql.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;

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
    private final Map<QualifiedAttributeID, Attribute> fromAttributesIds = new HashMap<>();

    private Expression whereClause;
    private final List<SelectItem> projection = new LinkedList<>();

    public List<RelationID> getRelations() {
        return relations;
    }

    private final List<RelationID> relations = new LinkedList<>();

    //endregion

    public SQLQueryParser(Select selectQuery, DBMetadata dbMetadata) {
        this.dbMetadata = dbMetadata;
        this.idFac =  this.dbMetadata.getQuotedIDFactory();

        // the WITH is not supported
        if (selectQuery.getWithItemsList() != null) {
            unsupported( selectQuery.getWithItemsList() );
        }

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

    private void unsupported(Object unsupportedObject )  {
        log.warn(this.getClass() + " DOES NOT SUPPORT " + unsupportedObject);
        throw new ParseException(unsupportedObject) ;
    }

    private void unsupportedMapping(Object unsupportedObject )  {
        log.warn(this.getClass() + " DOES NOT SUPPORT " + unsupportedObject);
        throw new MappingQueryException(unsupportedObject) ;
    }

    /**
     * This exceptions is throwing when the parser cannot support an operation
     */
    private static class ParseException extends RuntimeException {

		private static final long serialVersionUID = 1L;

        protected final Object unsupportedObject;
        
		public ParseException (Object unsupportedObject) {
            this.unsupportedObject = unsupportedObject;
        }
    }

    private static class MappingQueryException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        protected final Object unsupportedObject;

        public MappingQueryException (Object unsupportedObject) {
            this.unsupportedObject = unsupportedObject;
        }
    }

    private void visitSubSelect(SubSelect subSelect ){
        if (subSelect.getSelectBody() instanceof PlainSelect) {
            PlainSelect subSelBody = (PlainSelect) (subSelect.getSelectBody());

            if (subSelBody.getJoins() != null || subSelBody.getWhere() != null) {
                unsupported(subSelect);
            }
            else {
                subSelBody.accept(selectVisitor);
            }
        }
        else
            unsupported(subSelect);
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


    private void addFromAttributeIds(Attribute attribute){
        if (! fromAttributesIds.containsKey(attribute.getQualifiedID()))
            fromAttributesIds.put(attribute.getQualifiedID(), attribute );
    }

    private final SelectVisitor selectVisitor = new SelectVisitor() {
        @Override
        public void visit(PlainSelect plainSelect) {
            // the SELECT should correspond to a CQ
            validatePlainSelect(plainSelect);

            // projection
            for (SelectItem item : plainSelect.getSelectItems())
                item.accept(selectItemVisitor); // todo : review

            // from items
            plainSelect.getFromItem().accept(fromItemVisitor);

            joinVisit(plainSelect);

            if (plainSelect.getWhere() != null) {
                whereClause = plainSelect.getWhere();
                //we visit the where clause to fix any and all comparison
                whereClause.accept(whereExpressionVisitor);
                whereClause.accept(tableExpressionVisitor);
            }

        }
        @Override
        public void visit(SetOperationList setOpList) {
            unsupported(setOpList);
        }

        @Override
        public void visit(WithItem withItem) {
            unsupported(withItem);
        }

        /**
         * This validates that a PlainSelect Object is datalog suitable
         *
         * @param plainSelect Plain select query
         */
        private void validatePlainSelect(PlainSelect plainSelect) {
            if (plainSelect.getDistinct() != null) 
                unsupported(plainSelect.getDistinct());
            
            if (plainSelect.getIntoTables() != null)
                unsupportedMapping(plainSelect.getIntoTables());
            
            if (plainSelect.getHaving() != null) 
                unsupported(plainSelect.getHaving());
            
            if (plainSelect.getGroupByColumnReferences() != null) 
                unsupported(plainSelect.getGroupByColumnReferences());
            
            if (plainSelect.getOrderByElements() != null)
                unsupported(plainSelect.getOrderByElements());
            
            if (plainSelect.getLimit() != null) 
                unsupported(plainSelect.getLimit());

            if (plainSelect.getTop() != null) 
                unsupported(plainSelect.getTop());
                
            if (plainSelect.getOracleHierarchical() != null)
                unsupported(plainSelect.getOracleHierarchical());
            
            if (plainSelect.isOracleSiblings())
                unsupported(plainSelect.isOracleSiblings());
        }

        /**
         * This validates that a Joins List of Object is datalog suitable
         *
         * @param joins, list of Join objects
         */
        private void validateJoins ( List<Join> joins ){

            for ( Join join : joins) {
                if (join.isOuter())
                    unsupported(join.isOuter());

                if (join.isFull())
                    unsupported(join.isFull());

                if (join.isRight())
                    unsupported(join.isRight());

                if (join.isLeft())
                    unsupported(join.isLeft());
            }
        }


        private void addNewBinaryJoinCondition( Attribute leftAttribute, Attribute rightAttribute, String columnName,  BinaryExpression binaryExpression ){
            Column leftColumn = new Column(
                    new Table( leftAttribute.getRelation().getID().getSchemaName(),
                            leftAttribute.getRelation().getID().getTableName()), columnName );

            Column rightColumn = new Column(
                    new Table(  rightAttribute.getRelation().getID().getSchemaName(),
                            rightAttribute.getRelation().getID().getTableName()), columnName);
            rightColumn.accept(joinExpressionVisitor);

            binaryExpression.setLeftExpression(leftColumn);
            binaryExpression.setRightExpression(rightColumn);
            joinConditions.add(binaryExpression);

        }



        private void naturalJoinVisit( Join join ){
            RelationDefinition rightRd = getTableRelationDefinition(join.getRightItem());
            Map<QuotedID, Attribute> mappedRightAttributes = getMappedAttributeByQuotedId(rightRd.getAttributes());
            for ( Attribute leftAttribute :  fromAttributesIds.values()) {
                Attribute rightAttribute = null;
                if ( mappedRightAttributes.containsKey(leftAttribute.getID()) ){
                    rightAttribute = mappedRightAttributes.get(leftAttribute.getID());
                }

                if ( rightAttribute != null &&
                        ! leftAttribute.getQualifiedID().equals(rightAttribute.getQualifiedID()) &&
                        leftAttribute.getID().equals(rightAttribute.getID() )  &&
                        leftAttribute.getType()==rightAttribute.getType()){

                    addNewBinaryJoinCondition( leftAttribute, rightAttribute, leftAttribute.getID().getName(), new EqualsTo() );
                    break;
                }
            }

        }

        private void visitJoinUsingColumns(Join join ){

            RelationDefinition rightRd = getTableRelationDefinition( join.getRightItem() );
            List<Attribute> rightAttributes = rightRd.getAttributes();
            Map<QuotedID, Attribute> mappedRightAttributes = getMappedAttributeByQuotedId(rightAttributes);

            for (Column column : join.getUsingColumns()) {
                QuotedID attributeID = idFac.createAttributeID(column.getColumnName());
                if (mappedRightAttributes.containsKey(attributeID)) {
                    Attribute rightAttribute = mappedRightAttributes.get(attributeID);
                    Attribute leftAttribute = null;
                    for (Attribute lAttribute : fromAttributesIds.values()) {
                        if (  ! lAttribute.getQualifiedID().equals(rightAttribute.getQualifiedID() ) &&
                                attributeID.equals(lAttribute.getID()) &&
                                lAttribute.getType()==rightAttribute.getType()) {

                            leftAttribute = lAttribute;
                        }
                    }
                    if (leftAttribute == null) {
                        // todo: verify the correctness of rise an exception
                        unsupportedMapping(attributeID);
                    }
                    addNewBinaryJoinCondition( leftAttribute, rightAttribute, attributeID.getName(), new EqualsTo() );

                } else {
                    // todo: verify the correctness of rise an exception
                    unsupportedMapping(attributeID);
                }
            }

        }

        private RelationDefinition getTableRelationDefinition(FromItem rightItem){
            if ( ! (rightItem instanceof  Table) ) { unsupported(rightItem);}

            Table rightTable = (Table) rightItem;
            RelationID rightRelationId = idFac.createRelationID(rightTable.getSchemaName(), rightTable.getName());
            RelationDefinition rightRd = dbMetadata.getRelation(rightRelationId);

            if (rightRd == null ) { unsupportedMapping(rightRelationId);}

            return rightRd;
        }



        private void joinVisit(PlainSelect plainSelect) {
            List<Join> joins = plainSelect.getJoins();

            if (joins == null) {
                return;
            }

            validateJoins( joins );

            for (Join join : joins) {
                join.getRightItem().accept(fromItemVisitor);

                if (join.getUsingColumns() != null) {
                    visitJoinUsingColumns(join);
                }else if ( join.isNatural() ) {
                        naturalJoinVisit(join);
                }else if ( join.isCross()){
                    // TODO : create the a the common  attribute from DBMetadata
                }else if (join.getOnExpression() != null ) {
                        join.getOnExpression().accept(joinExpressionVisitor);
                }

            }
        }
    };

    private Map<QuotedID, Attribute> getMappedAttributeByQuotedId(Collection <Attribute> attributes ){
        Map<QuotedID,Attribute> map = new HashMap<>();
        for (Attribute i : attributes)
            map.put(i.getID(), i);
        return map;
    }


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

            expr.accept(aliasMapExpressionVisitor); //todo:  this visitor is used just for the subSelect

            expr.accept(projectorExpressionVisitor);


            Alias alias = selectExpr.getAlias();
            // all complex expressions in SELECT must be named (by aliases)
            if (!(expr instanceof Column) && alias == null)
                unsupported(selectExpr);

            // alias
            if (alias  != null) {
                // NORMALIZE EXPRESSION ALIAS NAME (ROMAN 9 Dec 2015: this should be done later; the visitor should not modify the query)
                QuotedID aliasName = idFac.createAttributeID(alias.getName());
                alias.setName(aliasName.getSQLRendering());
                expressionAlias.put(aliasName, expr);
            }


        }
    };

    // todo: remove all the commnet
    private final FromItemVisitor fromItemVisitor = new FromItemVisitor() {

		@Override
        public void visit(Table table) {

            RelationID relationId = idFac.createRelationID(table.getSchemaName(), table.getName());
            RelationDefinition relationDefinition = dbMetadata.getRelation(relationId);
            if (relationDefinition  == null ) {unsupportedMapping( relationId);}

            relations.add(relationId);

            for (Attribute attribute : relationDefinition.getAttributes()) {
                addFromAttributeIds(attribute);
            }


            Alias as = table.getAlias();
            RelationID aliasId = (as != null) ? idFac.createRelationID(null, as.getName()) : relationId;
            tableAlias.put(aliasId, relationId);

        }

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
            // TODO: 30/12/2015 Does it should be integrate in the visitSubselect
            // subSelect.getSelectBody().accept(selectVisitor);
        }



        @Override
        public void visit(SubJoin subjoin) {
            unsupported(subjoin);

        }

        @Override
        public void visit(LateralSubSelect lateralSubSelect) {
            unsupported(lateralSubSelect);
        }

        @Override
        public void visit(ValuesList valuesList) {
            unsupported(valuesList);
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
            unsupported(multiExprList);
            for (ExpressionList exprList : multiExprList.getExprList())
                exprList.accept(this);
        }

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }
    };

    private ExpressionVisitor aliasMapExpressionVisitor = new ExpressionVisitor() {

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }

        @Override
        public void visit(Column tableColumn) {
        }


        @Override
        public void visit(NullValue nullValue) {
        }

        @Override
        public void visit(Function function) {
        }

        @Override
        public void visit(JdbcParameter jdbcParameter) {
        }

        @Override
        public void visit(JdbcNamedParameter jdbcNamedParameter) {
        }

        @Override
        public void visit(DoubleValue doubleValue) {
        }

        @Override
        public void visit(LongValue longValue) {
        }

        @Override
        public void visit(DateValue dateValue) {
        }

        @Override
        public void visit(TimeValue timeValue) {
        }

        @Override
        public void visit(TimestampValue timestampValue) {
        }

        @Override
        public void visit(Parenthesis parenthesis) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(StringValue stringValue) {
        }

        @Override
        public void visit(Addition addition) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(Division division) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(Multiplication multiplication) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(Subtraction subtraction) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(AndExpression andExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(OrExpression orExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(Between between) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(EqualsTo equalsTo) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(GreaterThan greaterThan) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(InExpression inExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(IsNullExpression isNullExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(LikeExpression likeExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(MinorThan minorThan) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            // TODO Auto-generated method stub

        }


        @Override
        public void visit(CaseExpression caseExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(WhenClause whenClause) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(ExistsExpression existsExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(AllComparisonExpression allComparisonExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(AnyComparisonExpression anyComparisonExpression) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(Concat concat) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(Matches matches) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(CastExpression cast) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(Modulo modulo) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(AnalyticExpression aexpr) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(ExtractExpression eexpr) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(IntervalExpression iexpr) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(OracleHierarchicalExpression oexpr) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(RegExpMatchOperator arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(SignedExpression arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(JsonExpression arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
            // TODO Auto-generated method stub

        }
    };


    private final ExpressionVisitor tableExpressionVisitor = new ExpressionVisitor() {

        /*
         * We do the same procedure for all Binary Expressions
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Addition)
         */
        @Override
        public void visit(Addition addition) {
            visitBinaryExpression(addition);
        }

        @Override
        public void visit(AndExpression andExpression) {
            visitBinaryExpression(andExpression);
        }

        @Override
        public void visit(Between between) {
            between.getLeftExpression().accept(this);
            between.getBetweenExpressionStart().accept(this);
            between.getBetweenExpressionEnd().accept(this);
        }

        @Override
        public void visit(Column tableColumn) {
            //it does nothing here, everything is good
        }

        @Override
        public void visit(Division division) {
            visitBinaryExpression(division);
        }

        @Override
        public void visit(DoubleValue doubleValue) {
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            visitBinaryExpression(equalsTo);
        }

        @Override
        public void visit(Function function) {
            switch (function.getName().toLowerCase()) {
                case "regexp_like" :
                case "regexp_replace" :
                case "replace" :
                case "concat" :
                case "substr" :
                    for (Expression ex :function.getParameters().getExpressions())
                        ex.accept(this);
                    break;

                default:
                    unsupported(function);
                    break;
            }
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            visitBinaryExpression(greaterThan);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            visitBinaryExpression(greaterThanEquals);
        }

        @Override
        public void visit(InExpression inExpression) {
            inExpression.getLeftExpression().accept(this);
            inExpression.getRightItemsList().accept(itemsListVisitor);
        }

        @Override
        public void visit(IsNullExpression isNullExpression) {
        }

        @Override
        public void visit(JdbcParameter jdbcParameter) {
            unsupported(jdbcParameter);
        }

        @Override
        public void visit(LikeExpression likeExpression) {
            visitBinaryExpression(likeExpression);
        }

        @Override
        public void visit(ExistsExpression existsExpression) {
            existsExpression.getRightExpression().accept(this);
        }

        @Override
        public void visit(LongValue longValue) {
        }

        @Override
        public void visit(MinorThan minorThan) {
            visitBinaryExpression(minorThan);
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            visitBinaryExpression(minorThanEquals);
        }

        @Override
        public void visit(Multiplication multiplication) {
            visitBinaryExpression(multiplication);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            visitBinaryExpression(notEqualsTo);
        }

        @Override
        public void visit(NullValue nullValue) {
        }

        @Override
        public void visit(OrExpression orExpression) {
            visitBinaryExpression(orExpression);
        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);
        }

        @Override
        public void visit(StringValue stringValue) {
        }

        @Override
        public void visit(Subtraction subtraction) {
            visitBinaryExpression(subtraction);
        }

        private void visitBinaryExpression(BinaryExpression binaryExpression) {
            binaryExpression.getLeftExpression().accept(this);
            binaryExpression.getRightExpression().accept(this);
        }

        @Override
        public void visit(DateValue dateValue) {
        }

        @Override
        public void visit(TimestampValue timestampValue) {
        }

        @Override
        public void visit(TimeValue timeValue) {
        }


        @Override
        public void visit(CaseExpression caseExpression) {
            unsupported(caseExpression);
        }

        @Override
        public void visit(WhenClause whenClause) {
            unsupported(whenClause);
        }

        @Override
        public void visit(AllComparisonExpression allComparisonExpression) {
            unsupported(allComparisonExpression);
            allComparisonExpression.getSubSelect().getSelectBody().accept(selectVisitor);
        }

        @Override
        public void visit(AnyComparisonExpression anyComparisonExpression) {
            unsupported(anyComparisonExpression);
            anyComparisonExpression.getSubSelect().getSelectBody().accept(selectVisitor);
        }

        @Override
        public void visit(Concat concat) {
            visitBinaryExpression(concat);
        }

        @Override
        public void visit(Matches matches) {
            unsupported(matches);
            visitBinaryExpression(matches);
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            unsupported(bitwiseAnd);
            visitBinaryExpression(bitwiseAnd);
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            unsupported(bitwiseOr);
            visitBinaryExpression(bitwiseOr);
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            unsupported(bitwiseXor);
            visitBinaryExpression(bitwiseXor);
        }

        @Override
        public void visit(CastExpression cast) {
            cast.getLeftExpression().accept(this);
        }

        @Override
        public void visit(Modulo modulo) {
            unsupported(modulo);
            visitBinaryExpression(modulo);
        }

        @Override
        public void visit(AnalyticExpression analytic) {
            unsupported(analytic);
        }

        @Override
        public void visit(ExtractExpression eexpr) {
            unsupported(eexpr);
        }

        @Override
        public void visit(IntervalExpression iexpr) {
            unsupported(iexpr);
        }

        @Override
        public void visit(JdbcNamedParameter jdbcNamedParameter) {
            unsupported(jdbcNamedParameter);
        }

        @Override
        public void visit(OracleHierarchicalExpression arg0) {
            unsupported(arg0);
        }

        @Override
        public void visit(RegExpMatchOperator rexpr) {
//			unsupported = true;
//			visitBinaryExpression(rexpr);
        }


        @Override
        public void visit(SignedExpression arg0) {
            System.out.println("WARNING: SignedExpression   not implemented ");
            unsupported(arg0);
        }

        @Override
        public void visit(JsonExpression arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }
    };

    private final ExpressionVisitor projectorExpressionVisitor = new ExpressionVisitor() {
        @Override
        public void visit(NullValue nullValue) {
            // TODO Auto-generated method stub
        }

        /*
         * The system cannot support function currently (non-Javadoc)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.Function)
         * @link ConferenceConcatMySQLTest
         */
        @Override
        public void visit(Function function) {
            switch (function.getName().toLowerCase()) {
                case "regexp_like" :
                case "regexp_replace" :
                case "replace" :
                case "concat" :
                    //case "substr" :
                    for (Expression ex :function.getParameters().getExpressions())
                        ex.accept(this);
                    break;
                default:
                    unsupported(function);
            }

        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);
        }

        @Override
        public void visit(Addition addition) {
            visitBinaryExpression(addition);
        }

        @Override
        public void visit(Division division) {
            visitBinaryExpression(division);
        }

        @Override
        public void visit(Multiplication multiplication) {
            visitBinaryExpression(multiplication);
        }

        @Override
        public void visit(Subtraction subtraction) {
            visitBinaryExpression(subtraction);
        }

        @Override
        public void visit(SignedExpression arg0) {
            arg0.getExpression().accept(this);
        }

        @Override
        public void visit(AndExpression andExpression) {
            unsupported(andExpression);
        }

        @Override
        public void visit(OrExpression orExpression) {
            unsupported(orExpression);
        }

        @Override
        public void visit(Between between) {
            between.getLeftExpression().accept(this);
            between.getBetweenExpressionStart().accept(this);
            between.getBetweenExpressionEnd().accept(this);
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            unsupported(equalsTo);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            unsupported(greaterThan);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            unsupported(greaterThanEquals);
        }

        @Override
        public void visit(InExpression inExpression) {
            //Expression e = inExpression.getLeftExpression();
            ItemsList e1 = inExpression.getLeftItemsList();
            if (e1 instanceof SubSelect){
                ((SubSelect)e1).accept(this);
            }
            else if (e1 instanceof ExpressionList) {
                for (Expression expr : ((ExpressionList)e1).getExpressions()) {
                    expr.accept(this);
                }
            }
            else if (e1 instanceof MultiExpressionList) {
                for (ExpressionList exp : ((MultiExpressionList)e1).getExprList()){
                    for (Expression expr : exp.getExpressions()) {
                        expr.accept(this);
                    }
                }
            }
        }

        @Override
        public void visit(IsNullExpression isNullExpression) {
            unsupported(isNullExpression);
        }

        @Override
        public void visit(LikeExpression likeExpression) {
            unsupported(likeExpression);
        }

        @Override
        public void visit(MinorThan minorThan) {
            unsupported(minorThan);
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            unsupported(minorThanEquals);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            unsupported(notEqualsTo);
        }

        /*
         * Visit the column and remove the quotes if they are present(non-Javadoc)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
         */
        @Override
        public void visit(Column tableColumn) {
            // CHANGES TABLE AND COLUMN NAMES
            // TODO: add implementation here
            normalizeColumnName(idFac, tableColumn);
        }

        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }

        @Override
        public void visit(CaseExpression caseExpression) {
            unsupported(caseExpression);
        }

        @Override
        public void visit(WhenClause whenClause) {
            unsupported(whenClause);
        }

        @Override
        public void visit(ExistsExpression existsExpression) {
            unsupported(existsExpression);
        }

        @Override
        public void visit(AllComparisonExpression allComparisonExpression) {
            unsupported(allComparisonExpression);
        }

        @Override
        public void visit(AnyComparisonExpression anyComparisonExpression) {
            unsupported(anyComparisonExpression);
        }

        @Override
        public void visit(Concat concat) {
            visitBinaryExpression(concat);
        }

        @Override
        public void visit(Matches matches) {
            unsupported(matches);
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            unsupported(bitwiseAnd);
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            unsupported(bitwiseOr);
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            unsupported(bitwiseXor);
        }

        @Override
        public void visit(CastExpression cast) {


        }

        @Override
        public void visit(Modulo modulo) {
            unsupported(modulo);
        }

        @Override
        public void visit(AnalyticExpression aexpr) {
            unsupported(aexpr);
        }

        @Override
        public void visit(ExtractExpression eexpr) {
            unsupported(eexpr);
        }

        @Override
        public void visit(IntervalExpression iexpr) {
            unsupported(iexpr);
        }

        @Override
        public void visit(OracleHierarchicalExpression oexpr) {
            unsupported(oexpr);
        }

        @Override
        public void visit(RegExpMatchOperator arg0) {
            unsupported(arg0);
        }


        @Override
        public void visit(JsonExpression arg0) {
        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
        }

        @Override
        public void visit(JdbcParameter jdbcParameter) {
            unsupported(jdbcParameter);
        }

        @Override
        public void visit(JdbcNamedParameter jdbcNamedParameter) {
            unsupported(jdbcNamedParameter);
        }


        private void visitBinaryExpression(BinaryExpression binaryExpression) {
            binaryExpression.getLeftExpression().accept(this);
            binaryExpression.getRightExpression().accept(this);
        }

	/*
	 * scalar values: all supported
	 */

        @Override
        public void visit(DoubleValue doubleValue) {
            // NO-OP
        }

        @Override
        public void visit(LongValue longValue) {
            // NO-OP
        }

        @Override
        public void visit(DateValue dateValue) {
            // NO-OP
        }

        @Override
        public void visit(TimeValue timeValue) {
            // NO-OP
        }

        @Override
        public void visit(TimestampValue timestampValue) {
            // NO-OP
        }

        @Override
        public void visit(StringValue stringValue) {
            // NO-OP
        }
    };

    /**
     * Visitor for expression in the JOIN ON conditions
     *
     */
    private final ExpressionVisitor joinExpressionVisitor = new ExpressionVisitor() {
        @Override
        public void visit(NullValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(Function arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(JdbcParameter arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(JdbcNamedParameter arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(DoubleValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(LongValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(DateValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(TimeValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(TimestampValue arg0) {
            //we do not execute anything

        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);

        }

        @Override
        public void visit(StringValue arg0) {
            //we do not execute anything

        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Addition)
         */
        @Override
        public void visit(Addition addition) {
            visitBinaryExpression(addition);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Division)
         */
        @Override
        public void visit(Division arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Multiplication)
         */
        @Override
        public void visit(Multiplication arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.arithmetic.Subtraction)
         */
        @Override
        public void visit(Subtraction arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.AndExpression)
         */
        @Override
        public void visit(AndExpression arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.conditional.OrExpression)
         */
        @Override
        public void visit(OrExpression arg0) {
            visitBinaryExpression(arg0);
        }

        @Override
        public void visit(Between arg0) {
            //we do not consider the case of BETWEEN

        }

		/*
		 * We store in join conditions the binary expression that are not nested,
		 * for the others we continue to visit the subexpression
		 * Example: AndExpression and OrExpression always have subexpression.
		 */

        public void visitBinaryExpression(BinaryExpression binaryExpression) {
            Expression left = binaryExpression.getLeftExpression();
            Expression right = binaryExpression.getRightExpression();

            if (!( (left instanceof BinaryExpression) ||
                    (right instanceof BinaryExpression) )) {

                left.accept(this);
                right.accept(this);
                // ROMAN (25 Sep 2015): this transforms OR into AND
                joinConditions.add(binaryExpression);
            }
            else {
                left.accept(this);
                right.accept(this);
            }
        }

        /*
         *  We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.EqualsTo)
         */
        @Override
        public void visit(EqualsTo arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         *  We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThan)
         */
        @Override
        public void visit(GreaterThan arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         *  We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals)
         */
        @Override
        public void visit(GreaterThanEquals arg0) {
            visitBinaryExpression(arg0);
        }


        @Override
        public void visit(InExpression arg0) {
            //we do not support the case for IN condition

        }

        @Override
        public void visit(IsNullExpression arg0) {
            //we do not execute anything

        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.LikeExpression)
         */
        @Override
        public void visit(LikeExpression arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThan)
         */
        @Override
        public void visit(MinorThan arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(MinorThanEquals arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.NotEqualsTo)
         */
        @Override
        public void visit(NotEqualsTo arg0) {
            visitBinaryExpression(arg0);
        }

        /*
         * Remove quotes from columns if they are present (non-Javadoc)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
         */
        @Override
        public void visit(Column tableColumn) {
            // CHANGE TABLE / COLUMN NAME IN THE JOIN CONDITION
            // TableJSQL.unquoteColumnAndTableName(tableColumn);
           normalizeColumnName(idFac, tableColumn);
        }

        /*
         * We visit also the subselect to find nested joins
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.statement.select.SubSelect)
         */
        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }

        @Override
        public void visit(CaseExpression arg0) {
            // we do not support case expression
            unsupported(arg0);

        }

        @Override
        public void visit(WhenClause arg0) {
            // we do not support when expression
            unsupported(arg0);
        }

        @Override
        public void visit(ExistsExpression exists) {
            // we do not support exists
            unsupported(exists);
        }

        /*
         * We visit the subselect in ALL(...)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AllComparisonExpression)
         */
        @Override
        public void visit(AllComparisonExpression all) {
            unsupported(all);
        }

        /*
         * We visit the subselect in ANY(...)
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnyComparisonExpression)
         */
        @Override
        public void visit(AnyComparisonExpression any) {
            unsupported(any);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Concat arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Matches arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseAnd arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseOr arg0) {
            unsupported(arg0);
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(BitwiseXor arg0) {
            unsupported(arg0);
        }

        @Override
        public void visit(CastExpression arg0) {
            // TODO :  this should be ignored
        }

        /*
         * We handle in the same way all BinaryExpression
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.MinorThanEquals)
         */
        @Override
        public void visit(Modulo arg0) {
            unsupported(arg0);
        }

        @Override
        public void visit(AnalyticExpression arg0) {
            // we do not consider AnalyticExpression
            unsupported(arg0);
        }

        @Override
        public void visit(ExtractExpression arg0) {
            // we do not consider ExtractExpression
            unsupported(arg0);
        }

        @Override
        public void visit(IntervalExpression arg0) {
            // we do not consider IntervalExpression
            unsupported(arg0);
        }

        @Override
        public void visit(OracleHierarchicalExpression arg0) {
            // we do not consider OracleHierarchicalExpression
            unsupported(arg0);
        }


        @Override
        public void visit(RegExpMatchOperator arg0) {
            unsupported(arg0);
        }

        @Override
        public void visit(SignedExpression arg0) {
            unsupported(arg0);

        }

        @Override
        public void visit(JsonExpression arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
            // TODO Auto-generated method stub

        }
    };

    private final ExpressionVisitor whereExpressionVisitor = new ExpressionVisitor() {

        @Override
        public void visit(NullValue nullValue) {
            // we do not execute anything
        }

        @Override
        public void visit(Function function) {
            // ROMAN (22 Sep 2015): longer list of supported functions?
            if (function.getName().toLowerCase().equals("regexp_like")) {
                for (Expression ex :function.getParameters().getExpressions())
                    ex.accept(this);
            }
            else
                unsupported(function);
        }

        @Override
        public void visit(JdbcParameter jdbcParameter) {
            //we do not execute anything
        }

        @Override
        public void visit(JdbcNamedParameter jdbcNamedParameter) {
            // we do not execute anything
        }

        @Override
        public void visit(DoubleValue doubleValue) {
            // we do not execute anything
        }

        @Override
        public void visit(LongValue longValue) {
            // we do not execute anything
        }

        @Override
        public void visit(DateValue dateValue) {
            // we do not execute anything
        }

        @Override
        public void visit(TimeValue timeValue) {
            // we do not execute anything
        }

        @Override
        public void visit(TimestampValue timestampValue) {
            // we do not execute anything
        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);
        }

        @Override
        public void visit(StringValue stringValue) {
            // we do not execute anything
        }

        @Override
        public void visit(Addition addition) {
            visitBinaryExpression(addition);
        }

        @Override
        public void visit(Division division) {
            visitBinaryExpression(division);
        }

        @Override
        public void visit(Multiplication multiplication) {
            visitBinaryExpression(multiplication);
        }

        @Override
        public void visit(Subtraction subtraction) {
            visitBinaryExpression(subtraction);
        }

        @Override
        public void visit(AndExpression andExpression) {
            visitBinaryExpression(andExpression);
        }

        @Override
        public void visit(OrExpression orExpression) {
            visitBinaryExpression(orExpression);
        }

        @Override
        public void visit(Between between) {
            between.getLeftExpression().accept(this);
            between.getBetweenExpressionStart().accept(this);
            between.getBetweenExpressionEnd().accept(this);
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            visitBinaryExpression(equalsTo);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            visitBinaryExpression(greaterThan);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            visitBinaryExpression(greaterThanEquals);
        }

        @Override
        public void visit(InExpression inExpression) {

            //Expression e = inExpression.getLeftExpression();

            // ROMAN (25 Sep 2015): why not getLeftExpression? getLeftItemList can be empty
            // what about the right-hand side list?!

            ItemsList e1 = inExpression.getLeftItemsList();
            if (e1 instanceof SubSelect) {
                ((SubSelect)e1).accept((ExpressionVisitor)this);
            }
            else if (e1 instanceof ExpressionList) {
                for (Expression expr : ((ExpressionList)e1).getExpressions()) {
                    expr.accept(this);
                }
            }
            else if (e1 instanceof MultiExpressionList) {
                for (ExpressionList exp : ((MultiExpressionList)e1).getExprList()){
                    for (Expression expr : exp.getExpressions()) {
                        expr.accept(this);
                    }
                }
            }
        }

        /*
         * We add the content of isNullExpression in SelectionJSQL
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.operators.relational.InExpression)
         */
        @Override
        public void visit(IsNullExpression isNullExpression) {

        }

        @Override
        public void visit(LikeExpression likeExpression) {
            visitBinaryExpression(likeExpression);
        }

        @Override
        public void visit(MinorThan minorThan) {
            visitBinaryExpression(minorThan);
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            visitBinaryExpression(minorThanEquals);
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            visitBinaryExpression(notEqualsTo);
        }

    	/*
    	 * Visit the column and remove the quotes if they are present(non-Javadoc)
    	 * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.schema.Column)
    	 */

        @Override
        public void visit(Column tableColumn) {
            // CHANGES THE TABLE SCHEMA / NAME AND COLUMN NAME
            normalizeColumnName(idFac, tableColumn);
        }

        /*
         * we search for nested where in SubSelect
         * @see net.sf.jsqlparser.statement.select.FromItemVisitor#visit(net.sf.jsqlparser.statement.select.SubSelect)
         */
        @Override
        public void visit(SubSelect subSelect) {
            visitSubSelect(subSelect);
        }

        @Override
        public void visit(CaseExpression caseExpression) {
            // it is not supported

        }

        @Override
        public void visit(WhenClause whenClause) {
            // it is not supported

        }

        @Override
        public void visit(ExistsExpression existsExpression) {
            // it is not supported

        }

        /*
         * We add the content of AllComparisonExpression in SelectionJSQL
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AllComparisonExpression)
         */
        @Override
        public void visit(AllComparisonExpression allComparisonExpression) {

        }

        /*
         * We add the content of AnyComparisonExpression in SelectionJSQL
         * @see net.sf.jsqlparser.expression.ExpressionVisitor#visit(net.sf.jsqlparser.expression.AnyComparisonExpression)
         */
        @Override
        public void visit(AnyComparisonExpression anyComparisonExpression) {

        }

        @Override
        public void visit(Concat concat) {
            visitBinaryExpression(concat);
        }

        @Override
        public void visit(Matches matches) {
            visitBinaryExpression(matches);
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            visitBinaryExpression(bitwiseAnd);
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            visitBinaryExpression(bitwiseOr);
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            visitBinaryExpression(bitwiseXor);
        }

        @Override
        public void visit(Modulo modulo) {
            visitBinaryExpression(modulo);
        }


        @Override
        public void visit(CastExpression cast) {
            // not supported
        }

        @Override
        public void visit(AnalyticExpression aexpr) {
            // not supported
        }

        @Override
        public void visit(ExtractExpression eexpr) {
            // not supported
        }

        @Override
        public void visit(IntervalExpression iexpr) {
            // not supported
        }

        @Override
        public void visit(OracleHierarchicalExpression oexpr) {
            //not supported
        }

    	/*
    	 * We handle differently AnyComparisonExpression and AllComparisonExpression
    	 *  since they do not have a toString method, we substitute them with ad hoc classes.
    	 *  we continue to visit the subexpression.
    	 */

        private void visitBinaryExpression(BinaryExpression binaryExpression) {

            Expression left = binaryExpression.getLeftExpression();
            Expression right = binaryExpression.getRightExpression();

            if (right instanceof AnyComparisonExpression){
                right = new AnyComparison(((AnyComparisonExpression) right).getSubSelect());
                binaryExpression.setRightExpression(right);
            }

            if (right instanceof AllComparisonExpression){
                right = new AllComparison(((AllComparisonExpression) right).getSubSelect());
                binaryExpression.setRightExpression(right);
            }

            left.accept(this);
            right.accept(this);
        }

        @Override
        public void visit(SignedExpression arg0) {
            // do nothing
        }

        @Override
        public void visit(RegExpMatchOperator arg0) {
            // do nothing
        }

        @Override
        public void visit(RegExpMySQLOperator arg0) {
            // do nothing
        }

        @Override
        public void visit(JsonExpression arg0) {
            unsupported(arg0);
        }
    };

    //endregion


    //region Auxiliary Classes

    /**
     * Auxiliary Class used to visualize AnyComparison in string format.
     * Any and Some are the same in SQL so we consider always the case of ANY
     *
     */
    private final static class AllComparison extends AllComparisonExpression {

        public AllComparison(SubSelect subSelect) {
            super(subSelect);
        }

        @Override
        public String toString(){
            return "ALL "+ getSubSelect();
        }
    }

    private final static class AnyComparison extends AnyComparisonExpression {

        public AnyComparison(SubSelect subSelect) {
            super(subSelect);
        }

        @Override
        public String toString(){
            return "ANY "+ getSubSelect();
        }

    }
    //endregion

}
