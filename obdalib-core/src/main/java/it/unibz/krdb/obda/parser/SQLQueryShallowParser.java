package it.unibz.krdb.obda.parser;

import it.unibz.krdb.sql.QuotedIDFactory;
import it.unibz.krdb.sql.api.ShallowlyParsedSQLQuery;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;


public class SQLQueryShallowParser {

	/**
	 * Called from MappingParser and MetaMappingExpander. 
	 * Returns the query, even if there were parsing errors.
	 * 
	 * @param query The sq query to be parsed
	 * @return a ShallowlyParsedSQLQuery 
	 * @throws JSQLParserException 
	 */
	
	public static ShallowlyParsedSQLQuery parse(QuotedIDFactory idfac, String query) throws JSQLParserException {
        Statement st = CCJSqlParserUtil.parse(query);
        if (!(st instanceof Select))
        	throw new JSQLParserException("The inserted query is not a SELECT statement");

        ShallowlyParsedSQLQuery parsedQuery = new ShallowlyParsedSQLQuery((Select)st, idfac);
        return parsedQuery;
	}	
}
