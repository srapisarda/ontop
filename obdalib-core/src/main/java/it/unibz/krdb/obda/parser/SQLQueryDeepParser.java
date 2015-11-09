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

import it.unibz.krdb.sql.DBMetadata;
import it.unibz.krdb.sql.QualifiedAttributeID;
import it.unibz.krdb.sql.QuotedID;
import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.RelationID;
import it.unibz.krdb.sql.ParserViewDefinition;
import it.unibz.krdb.sql.api.DeeplyParsedSQLQuery;

import java.util.ArrayList;
import java.util.List;

import it.unibz.krdb.sql.api.ShallowlyParsedSQLQuery;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLQueryDeepParser {

	public static final String QUERY_NOT_SUPPORTED = "Query not yet supported";

	private static Logger log = LoggerFactory.getLogger(SQLQueryDeepParser.class);
	
	/**
	 * Called from MappingAnalyzer:createLookupTable. Returns the parsed query, or, if there are
	 * syntax error, the name of a generated view, even if there were 
	 * parsing errors. 
     *               If true, (i) deepParsing columns, (ii) create a view if an error is generated,
     *               and (iii) store information about the different part of the query
	 * 
	 * @param query The sql query to be parsed
	 * @return A ParsedQuery (or a SELECT * FROM table with the generated view)
	 */
	public static DeeplyParsedSQLQuery parse(DBMetadata dbMetaData, String query) {
    	
		boolean errors = false;
		DeeplyParsedSQLQuery queryParser = null;
		
		try {
			queryParser = new DeeplyParsedSQLQuery(query, dbMetaData.getQuotedIDFactory());
		} 
		catch (JSQLParserException e) {
			if (e.getCause() instanceof ParseException)
				log.warn("Parse exception, check no SQL reserved keywords have been used "+ e.getCause().getMessage());
			errors = true;
		}
		
		if (queryParser == null || errors) {
			log.warn("The following query couldn't be parsed. " +
					"This means Quest will need to use nested subqueries (views) to use this mappings. " +
					"This is not good for SQL performance, specially in MySQL. " + 
					"Try to simplify your query to allow Quest to parse it. " + 
					"If you think this query is already simple and should be parsed by Quest, " +
					"please contact the authors. \nQuery: '{}'", query);
			
			ParserViewDefinition viewDef = createViewDefinition(dbMetaData, query);
			queryParser = createParsedSqlForGeneratedView(dbMetaData.getQuotedIDFactory(), viewDef.getID());	
		}
		return queryParser;
	}


	/**
	 * creates a query of the form SELECT * FROM viewName
	 */

	static ShallowlyParsedSQLQuery createShallowlyParsedSqlForGeneratedView(QuotedIDFactory idfac, RelationID viewId) {
		Select select = getSelectParsedQuery(viewId);

		ShallowlyParsedSQLQuery queryParsed = null;
		try {
			queryParsed = new  ShallowlyParsedSQLQuery(select, idfac);
		}
		catch (JSQLParserException e) {
			if (e.getCause() instanceof ParseException)
				log.warn("Parse exception, check no SQL reserved keywords have been used "+ e.getCause().getMessage());
		}

		return queryParsed;
	}

	private static Select getSelectParsedQuery(RelationID viewId){
		PlainSelect body = new PlainSelect();

		List<SelectItem> list = new ArrayList<>(1);
		list.add(new AllColumns());
		body.setSelectItems(list); // create SELECT *

		Table viewTable = new Table(viewId.getSchemaSQLRendering(), viewId.getTableNameSQLRendering());
		body.setFromItem(viewTable); // create FROM viewTable

		Select select = new Select();
		select.setSelectBody(body);
		return select;
	}

	/**
	 * creates a query of the form SELECT * FROM viewName
	 */
    
	static DeeplyParsedSQLQuery createParsedSqlForGeneratedView(QuotedIDFactory idfac, RelationID viewId) {
		Select select = getSelectParsedQuery(viewId);

		DeeplyParsedSQLQuery queryParsed = null;
		try {
			queryParsed = new  DeeplyParsedSQLQuery(select, idfac);
		} 
		catch (JSQLParserException e) {
			if (e.getCause() instanceof ParseException)
				log.warn("Parse exception, check no SQL reserved keywords have been used "+ e.getCause().getMessage());
		}

		return queryParsed;
	}
	
	
	private static ParserViewDefinition createViewDefinition(DBMetadata md, String query) {

        QuotedIDFactory idfac = md.getQuotedIDFactory();
		
        ShallowlyParsedSQLQuery queryParser = null;
        boolean supported = true;
        try {
            queryParser = new ShallowlyParsedSQLQuery(query, idfac);
        } 
        catch (JSQLParserException e) {
            supported = false;
        }

        ParserViewDefinition viewDefinition = md.createParserView(query);
        
        if (supported) {
            List<Column> columns = queryParser.getColumns();
            for (Column column : columns) {
            	QuotedID columnId = idfac.createAttributeID(column.getColumnName());
            	RelationID relationId;
            	Table table = column.getTable();
            	if (table == null) // this column is an alias 
            		relationId = viewDefinition.getID();
            	else
            		relationId = idfac.createRelationID(table.getSchemaName(), table.getName());
            	
                viewDefinition.addAttribute(new QualifiedAttributeID(relationId, columnId));
            }
        }
        else {
            int start = "select".length(); 
            int end = query.toLowerCase().indexOf("from");
            if (end == -1) 
                throw new RuntimeException("Error parsing SQL query: Couldn't find FROM clause");
         

            String projection = query.substring(start, end).trim();

            //split where comma is present but not inside parenthesis
		    String[] columns = projection.split(",+(?!.*\\))");
//            String[] columns = projection.split(",+(?![^\\(]*\\))");


            for (String col : columns) {
                String columnName = col.trim();
			
    			/*
    			 * Take the alias name if the column name has it.
    			 */
                final String[] aliasSplitters = new String[] { " as ",  " AS " };

                for (String aliasSplitter : aliasSplitters) {
                    if (columnName.contains(aliasSplitter)) { // has an alias
                        columnName = columnName.split(aliasSplitter)[1].trim();
                        break;
                    }
                }
                ////split where space is present but not inside single quotes
                if (columnName.contains(" "))
                    columnName = columnName.split("\\s+(?![^'\"]*')")[1].trim();
                
    			// Get only the short name if the column name uses qualified name.
    			// Example: table.column -> column
                if (columnName.contains(".")) {
                    columnName = columnName.substring(columnName.lastIndexOf(".") + 1); // get only the name
                }
                // TODO (ROMAN 20 Oct 2015): extract schema and table name as well

                QuotedID columnId = idfac.createAttributeID(columnName);

                viewDefinition.addAttribute(new QualifiedAttributeID(null, columnId)); 
            }
        }
        return viewDefinition;
	}
}
