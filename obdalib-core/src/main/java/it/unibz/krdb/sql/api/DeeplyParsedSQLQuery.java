package it.unibz.krdb.sql.api;

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

import it.unibz.krdb.obda.parser.*;
import it.unibz.krdb.sql.QuotedID;
import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.RelationID;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;
import java.util.Map;

/**
 * A structure to store the parsed SQL query string. It returns the information
 * about the query using the visitor classes
 */
public class DeeplyParsedSQLQuery  {

    private final Select selectQuery; // the parsed query

    // maps aliases or relation names to relation names (identity on the relation names)
    private final Map<RelationID, RelationID> tables;

    private final Map<QuotedID, Expression> aliasMap;
    private final List<Expression> joins;
    private final Expression whereClause;
    private final List<SelectItem> projection;


    /**
     * Parse deeply a statement
     *
     * @param statement
     *            we pass already a parsed statement
     * @param idfac
     *         QuotedIDFactory object
     * @throws net.sf.jsqlparser.JSQLParserException
     */
    public DeeplyParsedSQLQuery(Select statement, QuotedIDFactory idfac) throws JSQLParserException {
        selectQuery = statement;

        TableNameVisitor tableNameVisitor = new TableNameVisitor(selectQuery, idfac);
        tables =  tableNameVisitor.getTables();
        if (!tableNameVisitor.isSupported())
            throw new JSQLParserException(SQLQueryDeepParser.QUERY_NOT_SUPPORTED);

        WhereClauseVisitor whereClauseVisitor = new WhereClauseVisitor(selectQuery, idfac);
        whereClause = whereClauseVisitor.getWhereClause(); // bring the names in WHERE clause into NORMAL FORM
        if (!whereClauseVisitor.isSupported())
            throw new JSQLParserException(SQLQueryDeepParser.QUERY_NOT_SUPPORTED);

        ProjectionVisitor projectionVisitor = new ProjectionVisitor(selectQuery, idfac );
        projection = projectionVisitor.getProjection(); // bring the names in FROM clause into NORMAL FORM
        if (!projectionVisitor.isSupported())
            throw new JSQLParserException(SQLQueryDeepParser.QUERY_NOT_SUPPORTED);

        JoinConditionVisitor joinConditionVisitor = new JoinConditionVisitor(selectQuery, idfac);
        joins = joinConditionVisitor.getJoinConditions(); // bring the names in JOIN clauses into NORMAL FORM
        if (!projectionVisitor.isSupported())
            throw new JSQLParserException(SQLQueryDeepParser.QUERY_NOT_SUPPORTED);

        AliasMapVisitor aliasMapVisitor = new AliasMapVisitor(selectQuery, idfac);
        aliasMap = aliasMapVisitor.getAliasMap();    // bring the alias names in Expr AS Alias into NORMAL FORM
    }

    /**
     * Returns all the tables in this query (RO now).
     *
     * USED FOR CREATING DATALOG RULES AND PROVIDING METADATA WITH THE LIST OF TABLES
     *
     */
    public Map<RelationID, RelationID> getTables() {
        return tables;
    }

    @Override
    public String toString() {
        return selectQuery.toString();
    }

    /**
     * Get the object construction for the WHERE clause.
     *
     * CREATING DATALOG RULES
     * AND META-MAPPING EXPANDER
     *
     */
    public Expression getWhereClause() {
        return whereClause;
    }

    /**
     * Get the object construction for the SELECT clause (CHANGES TABLE AND COLUMN NAMES).
     *
     * CREATING DATALOG RULES
     * AND META-MAPPING EXPANDER
     *
     */
    public List<SelectItem> getProjection()  {
        return projection;
    }

    /**
     * Get the string construction of alias name.
     *
     * CREATING DATALOG RULES (LOOKUP TABLE)
     *
     * MAPS EXPRESSION -> NAME
     *
     */
    public Map<QuotedID, Expression> getAliasMap() {
        return aliasMap;
    }

    /**
     * Get the string construction of the join condition. The string has the
     * format of "VAR1=VAR2".
     *
     * CREATING DATALOG RULES (JOIN CONDITIONS)
     *
     */
    public List<Expression> getJoinConditions() {
        return joins;
    }
}
