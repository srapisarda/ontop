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
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
* A structure to store the parsed SQL query string. It returns the information
* about the query using the visitor classes
*/
public class DeeplyParsedSQLQuery implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2387850683528860103L;

	private Select selectQuery; // the parsed query

    private final QuotedIDFactory idfac;

    // maps aliases or relation names to relation names (identity on the relation names)
    private Map<RelationID, RelationID> tables;

    //TODO: SALVO - this field is set but unused
	//private List<RelationID> relations;

    private Map<QuotedID, Expression> aliasMap;
    private List<Expression> joins;
    private Expression whereClause;
    private ProjectionJSQL projection;

    /**
     * Parse deeply a query given as a String
     *
     * @param queryString
     *          the SQL query to parse
     * @param idfac
     *         QuotedIDFactory object
     * @throws JSQLParserException
     */
    public DeeplyParsedSQLQuery(String queryString, QuotedIDFactory idfac) throws JSQLParserException {
        this(CCJSqlParserUtil.parse(queryString), idfac );
    }

    /**
     * Parse deeply a statement
     *
     * @param statement
     *            we pass already a parsed statement
     * @param idfac
     *         QuotedIDFactory object
     * @throws net.sf.jsqlparser.JSQLParserException
     */
    public DeeplyParsedSQLQuery(Statement statement, QuotedIDFactory idfac) throws JSQLParserException {
        this.idfac = idfac;
        if (statement instanceof Select) {
            selectQuery = (Select) statement;
            tables = getTables();
            whereClause = getWhereClause(); // bring the names in WHERE clause into NORMAL FORM
            projection = getProjection(); // bring the names in FROM clause into NORMAL FORM
            joins = getJoinConditions(); // bring the names in JOIN clauses into NORMAL FORM
            aliasMap = getAliasMap();    // bring the alias names in Expr AS Alias into NORMAL FORM
        }
        // catch exception about wrong inserted columns
        else
            throw new JSQLParserException("The inserted query is not a SELECT statement");
    }

    /**
     * Returns all the tables in this query (RO now).
     *
     * USED FOR CREATING DATALOG RULES AND PROVIDING METADATA WITH THE LIST OF TABLES
     *
     */
    public Map<RelationID, RelationID> getTables() throws JSQLParserException {

        if (tables == null) {
            TableNameVisitor visitor = new TableNameVisitor(selectQuery, false, idfac);
            tables = visitor.getTables();
            // TODO: SALVO - this field is set but unused
            // relations = visitor.getRelations();
        }
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
     * @throws JSQLParserException
     */
    public Expression getWhereClause() throws JSQLParserException {
        if (whereClause == null) {
            WhereClauseVisitor visitor = new WhereClauseVisitor(idfac);
            // CHANGES TABLE SCHEMA / NAME / ALIASES AND COLUMN NAMES
            whereClause = visitor.getWhereClause(selectQuery, true);
        }
        return whereClause;
    }

    /**
     * Set the object construction for the WHERE clause, modifying the current
     * statement
     *
     * META-MAPPING EXPANDER
     *
     * @param whereClause Expression object
     */

    public void setWhereClause(Expression whereClause) {
        WhereClauseVisitor sel = new WhereClauseVisitor(idfac);
        sel.setWhereClause(selectQuery, whereClause);
        this.whereClause = whereClause;
    }

    /**
     * Get the object construction for the SELECT clause (CHANGES TABLE AND COLUMN NAMES).
     *
     * CREATING DATALOG RULES
     * AND META-MAPPING EXPANDER
     *
     * @throws JSQLParserException
     */
    public ProjectionJSQL getProjection() throws JSQLParserException {
        if (projection == null) {
            ProjectionVisitor visitor = new ProjectionVisitor(idfac);
            projection = visitor.getProjection(selectQuery, true);
        }
        return projection;

    }

    /**
     * Set the object construction for the SELECT clause, modifying the current
     * statement
     *
     * META-MAPPING EXPANDER
     *
     * @param projection select list or select distinct list
     */

    public void setProjection(ProjectionJSQL projection) {
        ProjectionVisitor visitor = new ProjectionVisitor(idfac);
        visitor.setProjection(selectQuery, projection);
        this.projection = projection;
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
        if (aliasMap == null) {
            AliasMapVisitor visitor = new AliasMapVisitor(selectQuery, idfac);
            aliasMap = visitor.getAliasMap();
        }
        return aliasMap;
    }

    /**
     * Get the string construction of the join condition. The string has the
     * format of "VAR1=VAR2".
     *
     * CREATING DATALOG RULES (JOIN CONDITIONS)
     *
     */
    public List<Expression> getJoinConditions() throws JSQLParserException {
        if (joins == null) {
            JoinConditionVisitor visitor = new JoinConditionVisitor(selectQuery, true, idfac);
            joins = visitor.getJoinConditions();
        }
        return joins;
    }

    /**
     * Get the list of columns (RO)
     *
     * ONLY FOR CREATING VIEWS!
     *
     * @return List of parsed columns
     */
    public List<Column> getColumns() {
        ColumnsVisitor visitor = new ColumnsVisitor(selectQuery);
        return visitor.getColumns();
    }


}
