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

import it.unibz.krdb.obda.parser.ColumnsVisitor;
import it.unibz.krdb.obda.parser.ProjectionVisitor;
import it.unibz.krdb.obda.parser.TableNameVisitor;
import it.unibz.krdb.obda.parser.WhereClauseVisitor;
import it.unibz.krdb.sql.QuotedID;
import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.RelationID;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
* A structure to store the parsed SQL query string. It returns the information
* about the query using the visitor classes
*/
public class ShallowlyParsedSQLQuery implements Serializable {


    /**
	 * 
	 */
	private static final long serialVersionUID = 4949346265386700162L;
	private final QuotedIDFactory idfac;
    //private String query;
    private Select selectQuery;

    // maps aliases or relation names to relation names (identity on the relation names)
    private Map<RelationID, RelationID> tables;
    private List<RelationID> relations;
    private ProjectionJSQL projection;
    private Expression whereClause;



     /**
     * Parse a query given as a String
     *
     * @param queryString
     *          the SQL query to parse
     * @param idfac
     *         QuotedIDFactory object
     * @throws JSQLParserException
     */
    public ShallowlyParsedSQLQuery(String queryString, QuotedIDFactory idfac) throws JSQLParserException {
        this(CCJSqlParserUtil.parse(queryString), idfac );
    }

    /**
     * Parse a statement
     *
     * @param statement
     *            we pass already a parsed statement
     * @param idfac
     *         QuotedIDFactory object
     * @throws net.sf.jsqlparser.JSQLParserException
     */
    public ShallowlyParsedSQLQuery(Statement statement, QuotedIDFactory idfac) throws JSQLParserException {
        this.idfac = idfac;
        //query = statement.toString();
        //init(statement);
        if (statement instanceof Select) {
            selectQuery = (Select) statement;
        } // catch exception about wrong inserted columns
        else
            throw new JSQLParserException("The inserted query is not a SELECT statement");
    }

    @Override
    public String toString() {
        return selectQuery.toString();
    }

    /**
     * Get the object construction for the SELECT clause (CHANGES TABLE AND COLUMN NAMES).
     *
     * CREATING DATALOG RULES
     * AND META-MAPPING EXPANDER
     *
     */
    public ProjectionJSQL getProjection()  {
        if (projection == null) {
            ProjectionVisitor visitor = new ProjectionVisitor(idfac);
            projection = visitor.getProjection(selectQuery);
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
     * Returns all the tables in this query (RO now).
     *
     * USED FOR CREATING DATALOG RULES AND PROVIDING METADATA WITH THE LIST OF TABLES
     *
     */
    public Map<RelationID, RelationID> getTables() {
        if (tables == null) {
            TableNameVisitor visitor = new TableNameVisitor(selectQuery, idfac);
            tables = visitor.getTables();
            relations = visitor.getRelations();
        }
        return tables;
    }

    /**
     * Relations parsed query contains
     *
     * @return all the relations the parsed query contains
     */
    public List<RelationID> getRelations()  {
        getTables();
        return relations;
    }

    /**
     *
     * @return Select object contains the parsed query
     */
    public Select getStatement() {
        return selectQuery;
    }

    /**
     * Get the object construction for the WHERE clause.
     *
     * CREATING DATALOG RULES
     * AND META-MAPPING EXPANDER
     *
     */
    public Expression getWhereClause() {
        if (whereClause == null) {
            WhereClauseVisitor visitor = new WhereClauseVisitor(idfac);
            // CHANGES TABLE SCHEMA / NAME / ALIASES AND COLUMN NAMES
            whereClause = visitor.getWhereClause(selectQuery);
        }
        return whereClause;
    }

    /**
     * Get the list of columns (RO)
     *
     * ONLY FOR CREATING VIEWS!
     *
     * @return  List of parsed query columns
     */
    public List<Column> getColumns() {
        ColumnsVisitor visitor = new ColumnsVisitor(selectQuery);
        return visitor.getColumns();
    }

    /**
	 *
	 * @param idfac QuotedIDFactory object
	 * @param tableColumn query columns
	 */
	public static void normalizeColumnName(QuotedIDFactory idfac, Column tableColumn) {
		QuotedID columnName = idfac.createAttributeID(tableColumn.getColumnName());
		tableColumn.setColumnName(columnName.getSQLRendering());

		Table table = tableColumn.getTable();
		RelationID tableName = idfac.createRelationID(table.getSchemaName(), table.getName());
		table.setSchemaName(tableName.getSchemaSQLRendering());
		table.setName(tableName.getTableNameSQLRendering());
	}
}
