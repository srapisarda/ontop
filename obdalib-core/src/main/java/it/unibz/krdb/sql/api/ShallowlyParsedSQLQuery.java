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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

/**
* A structure to store the parsed SQL query string. It returns the information
* about the query using the visitor classes
*/
public class ShallowlyParsedSQLQuery {


	private final QuotedIDFactory idfac;
    private Select selectQuery;

    private Expression whereClause;



    /**
     * Parse a statement
     *
     * @param statement
     *            we pass already a parsed statement
     * @param idfac
     *         QuotedIDFactory object
     */
    public ShallowlyParsedSQLQuery(Select statement, QuotedIDFactory idfac) {
        this.idfac = idfac;
        this.selectQuery = statement;
    }

    /**
     *
     * @return
     *  Object ShallowlyParsedSQLQuery copy of the current class instance
     */
    public ShallowlyParsedSQLQuery copy(List<SelectItem> projection, boolean distinct, Expression whereClause) {
    	try {
        	String query = selectQuery.toString();
        	
			ShallowlyParsedSQLQuery copy = new ShallowlyParsedSQLQuery((Select)CCJSqlParserUtil.parse(query), idfac);

			PlainSelect plainSelect = (PlainSelect)(copy.selectQuery.getSelectBody());
            plainSelect.getSelectItems().clear();
            if (!projection.isEmpty()) {
                plainSelect.getSelectItems().addAll(projection);
            } else {
                plainSelect.getSelectItems().add(new AllColumns());
            }
			if (distinct)
				plainSelect.setDistinct(new Distinct());

	        if (whereClause != null) {
                plainSelect.setWhere(whereClause);
	        }
	        return copy;
		} 
    	catch (JSQLParserException ignored) {
		}
    	// the exception should never happen
    	throw new NullPointerException();
    }
    
    @Override
    public String toString() {
        return selectQuery.toString();
    }

    /**
     * Get the object construction for the SELECT clause (CHANGES TABLE AND COLUMN NAMES).
     *
     * AND META-MAPPING EXPANDER
     *
     */
    public List<SelectItem> getProjection()  {
        ProjectionVisitor visitor = new ProjectionVisitor(selectQuery, idfac);
        return visitor.getProjection();
    }



    /**
     * Relations parsed query contains
     *
     * ONLY MAPPING PARSER (to obtain the list of tables)
     *
     * @return all the relations the parsed query contains
     */
    public List<RelationID> getRelations()  {
        TableNameVisitor visitor = new TableNameVisitor(selectQuery, idfac);
        return visitor.getRelations();
    }


    /**
     * Get the object construction for the WHERE clause.
     *
     * META-MAPPING EXPANDER
     *
     */
    public Expression getWhereClause() {
    	// this needs to be cached -- called many times in MetaMappingExpander
        if (whereClause == null) {
            WhereClauseVisitor visitor = new WhereClauseVisitor(selectQuery, idfac);
            // CHANGES TABLE SCHEMA / NAME / ALIASES AND COLUMN NAMES
            whereClause = visitor.getWhereClause();
        }
        return whereClause;
    }

    /**
     * Get the list of columns (RO)
     *
     * ONLY FOR CREATING VIEWS (in DeepSQLQueryParser)!
     *
     * @return  List of parsed query columns
     */
    public List<Column> getColumns() {
        ColumnsVisitor visitor = new ColumnsVisitor(selectQuery);
        return visitor.getColumns();
    }


}
