package org.semanticweb.ontop.mongo;

import java.sql.Types;
import java.util.Collection;
import java.util.List;

import org.semanticweb.ontop.model.BooleanOperationPredicate;
import org.semanticweb.ontop.model.Constant;
import org.semanticweb.ontop.model.DataTypePredicate;
import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.NonBooleanOperationPredicate;
import org.semanticweb.ontop.model.NumericalOperationPredicate;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URIConstant;
import org.semanticweb.ontop.model.ValueConstant;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.Predicate.COL_TYPE;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.HSQLSQLDialectAdapter;
import org.semanticweb.ontop.owlrefplatform.core.sql.SQLGenerator.QueryAliasIndex;
import org.semanticweb.ontop.owlrefplatform.core.srcquerygeneration.NativeQueryGenerator;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.TableDefinition;

public class MongoQueryGenerator implements NativeQueryGenerator {

	/**
	 * Operator symbols
	 */
	private static final String EQ_OPERATOR = "\"%s\" : %s";
	private static final String NEQ_OPERATOR = "\"%s\" : {$ne : %s}";
	private static final String GT_OPERATOR = "\"%s\" : {$gt : %s}";
	private static final String GTE_OPERATOR = "\"%s\" : {$gte : %s}";
	private static final String LT_OPERATOR = "\"%s\" : {$lt : %s}";
	private static final String LTE_OPERATOR = "\"%s\" : {$lte : %s}";
	private static final String AND_OPERATOR = "%s, %s";
	private static final String OR_OPERATOR = "$or : [ {%s}, {%s} ]";
	private static final String NOT_OPERATOR = "\"%s\" : {$not : %s}";
	
	//http://docs.mongodb.org/manual/faq/developers/#faq-developers-query-for-nulls
	private static final String IS_NULL_OPERATOR = "\"%s\" : {$type : 10}";
	private static final String IS_NOT_NULL_OPERATOR = "\"%s\" : {$not : {$type : 10} }";

	private static final String ADD_OPERATOR = "%s + %s";
	private static final String SUBSTRACT_OPERATOR = "%s - %s";
	private static final String MULTIPLY_OPERATOR = "%s * %s";

	private static final String LIKE_OPERATOR = "%s LIKE %s";

	private static final String INDENT = "    ";

	private static final String IS_TRUE_OPERATOR = "%s IS TRUE";

	
	private final DBMetadata dbMetadata;
	
	public MongoQueryGenerator(DBMetadata metadata) {
		dbMetadata = metadata.clone();
	}
	
	@Override
	public String generateSourceQuery(DatalogProgram query,
			List<String> signature) throws OBDAException {

		
		String mongo = "find(%s)";
		String selectPart = "";
		mongo = String.format (mongo, selectPart);
		return mongo;
	}

	/***
	 * Returns the Mongo criteria for an atom representing a Boolean condition.
	 */
	private String getMongoCriteria(Function atom, QueryAliasIndex index) {
		Predicate functionSymbol = atom.getFunctionSymbol();
		if (atom.isBooleanFunction()) {
			String expressionFormat = getBooleanOperatorString(functionSymbol);
			
			if (atom.getArity() == 1) {
			// For unary boolean operators, e.g., NOT, IS NULL, IS NOT NULL.
			// added also for IS TRUE
			
				Term term = atom.getTerm(0);
				String column = getColumnName(term, index);
			
				return String.format(expressionFormat, column);
			} else if (atom.getArity() == 2) {
				// For binary boolean operators, e.g., AND, OR, EQ, GT, LT, etc.
				// _
				Term left = atom.getTerm(0);//should be a key
				Term right = atom.getTerm(1);//should be a value
				String leftOp = getColumnName(left, index);
				String rightOp = getValue(right, index);

				return String.format(expressionFormat, leftOp, rightOp);
			}
		} else {
	/*		} else if (atom.isArithmeticFunction()) {
				// For numerical operators, e.g., MUTLIPLY, SUBSTRACT, ADDITION
				String expressionFormat = getNumericalOperatorString(functionSymbol);
				Term left = atom.getTerm(0);
				Term right = atom.getTerm(1);
				String leftOp = getSQLString(left, index, true);
				String rightOp = getSQLString(right, index, true);
				return String.format(expressionFormat, leftOp, rightOp);
			} else {
				throw new RuntimeException("The binary function "
						+ functionSymbol.toString() + " is not supported yet!");
			}
		} else {*/
/*			if (functionSymbol == OBDAVocabulary.SPARQL_REGEX) {
				boolean caseinSensitive = false;
				boolean multiLine = false;
				boolean dotAllMode = false;
				if (atom.getArity() == 3) {
					if (atom.getTerm(2).toString().contains("i")) {
						caseinSensitive = true;
					}
					if (atom.getTerm(2).toString().contains("m")) {
						multiLine = true;
					}
					if (atom.getTerm(2).toString().contains("s")) {
						dotAllMode = true;
					}
				}
				Term p1 = atom.getTerm(0);
				Term p2 = atom.getTerm(1);

				String column = getSQLString(p1, index, false);
				String pattern = getSQLString(p2, index, false);
				String sqlRegex = sqladapter.sqlRegex(column, pattern, caseinSensitive,
						multiLine, dotAllMode);
				return sqlRegex;
			} else {
*/				throw new RuntimeException("The builtin function "
						+ functionSymbol.toString() + " is not supported yet!");
//			}
		}
		return null;
	}

	private String getValue(Term right, QueryAliasIndex index) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Returns the Mongo template string for the boolean operator, including placeholders
	 * for the terms to be used, e.g., %s = %s, %s IS NULL, etc.
	 * 
	 * @param functionSymbol
	 * @return
	 */
	//TODO: almost a copy of the same method in SQLGenerator. Should we generalize it?
	private String getBooleanOperatorString(Predicate functionSymbol) {
		String operator = null;
		if (functionSymbol.equals(OBDAVocabulary.EQ)) {
			operator = EQ_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.NEQ)) {
			operator = NEQ_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.GT)) {
			operator = GT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.GTE)) {
			operator = GTE_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.LT)) {
			operator = LT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.LTE)) {
			operator = LTE_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.AND)) {
			operator = AND_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.OR)) {
			operator = OR_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.NOT)) {
			operator = NOT_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.IS_NULL)) {
			operator = IS_NULL_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.IS_NOT_NULL)) {
			operator = IS_NOT_NULL_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.IS_TRUE)) {
			operator = IS_TRUE_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.SPARQL_LIKE)) {
			operator = LIKE_OPERATOR;
		} else if (functionSymbol.equals(OBDAVocabulary.SPARQL_REGEX)) {
			operator = ""; //we do not need the operator for regex, it should not be used, because the sql adapter will take care of this
		} 
		else {
			throw new RuntimeException("Unknown boolean operator: " + functionSymbol);
		}
		return operator;
	}

	
	
	private String getColumnName(Term term, QueryAliasIndex index) {
		if (! (term instanceof Variable) ) {
			//TODO
		}
		
		Variable var = (Variable) term;
		Collection<String> posList = index.getColumnReferences(var);
		if (posList == null || posList.size() == 0) {
			throw new RuntimeException("Unbound variable found in WHERE clause: " + term);
		}
		return posList.iterator().next();
	}
	
	/**
	 * Generates the SQL string that forms or retrieves the given term. The
	 * function takes as input either: a constant (value or URI), a variable, or
	 * a Function (i.e., uri(), eq(..), ISNULL(..), etc)).
	 * <p>
	 * If the input is a constant, it will return the SQL that generates the
	 * string representing that constant.
	 * <p>
	 * If its a variable, it returns the column references to the position where
	 * the variable first appears.
	 * <p>
	 * If its a function uri(..) it returns the SQL string concatenation that
	 * builds the result of uri(...)
	 * <p>
	 * If its a boolean comparison, it returns the corresponding SQL comparison.
	 */
	public String getSQLString(Term term, QueryAliasIndex index, boolean useBrackets) {
		if (term == null) {
			return "";
		}
		if (term instanceof ValueConstant) {
			ValueConstant ct = (ValueConstant) term;
			if (isSI) {
				if (ct.getType() == COL_TYPE.OBJECT
						|| ct.getType() == COL_TYPE.LITERAL) {
					int id = getUriid(ct.getValue());
					if (id >= 0)
						//return jdbcutil.getSQLLexicalForm(String.valueOf(id));
						return String.valueOf(id);
				}
			}
			return jdbcutil.getSQLLexicalForm(ct);
		} else if (term instanceof URIConstant) {
			if (isSI) {
				String uri = term.toString();
				int id = getUriid(uri);
				return jdbcutil.getSQLLexicalForm(String.valueOf(id));
			}
			URIConstant uc = (URIConstant) term;
			return jdbcutil.getSQLLexicalForm(uc.toString());
		} else if (term instanceof Variable) {
			Variable var = (Variable) term;
			Collection<String> posList = index.getColumnReferences(var);
			if (posList == null || posList.size() == 0) {
				throw new RuntimeException(
						"Unbound variable found in WHERE clause: " + term);
			}
			return posList.iterator().next();
		}

		/* If its not constant, or variable its a function */

		Function function = (Function) term;
		Predicate functionSymbol = function.getFunctionSymbol();
		Term term1 = function.getTerms().get(0);
		int size = function.getTerms().size();

		if (functionSymbol instanceof DataTypePredicate) {
			if (functionSymbol.getType(0) == COL_TYPE.UNSUPPORTED) {
				throw new RuntimeException("Unsupported type in the query: "
						+ function);
			}
			if (size == 1) {
				// atoms of the form integer(x)
				return getSQLString(term1, index, false);
			} else {
				return getSQLStringForTemplateFunction(function, index);
			}
		} else if (functionSymbol instanceof BooleanOperationPredicate) {
			// atoms of the form EQ(x,y)
			String expressionFormat = getBooleanOperatorString(functionSymbol);
			if (isUnary(function)) {
				// for unary functions, e.g., NOT, IS NULL, IS NOT NULL
				// also added for IS TRUE
				if (expressionFormat.contains("IS TRUE")) {
					// find data type of term and evaluate accordingly
					String column = getSQLString(term1, index, false);
					int type = getVariableDataType(term1, index);
					if (type == Types.INTEGER)
						return String.format("%s > 0", column);
					if (type == Types.DOUBLE)
						return String.format("%s > 0", column);
					if (type == Types.BOOLEAN)
						return String.format("%s", column);
					if (type == Types.VARCHAR)
						return String.format("LENGTH(%s) > 0", column);
					return "1";
				}
				String op = getSQLString(term1, index, true);
				return String.format(expressionFormat, op);

			} else if (isBinary(function)) {
				// for binary functions, e.g., AND, OR, EQ, NEQ, GT, etc.
				String leftOp = getSQLString(term1, index, true);
				Term term2 = function.getTerms().get(1);
				String rightOp = getSQLString(term2, index, true);
				String result = String
						.format(expressionFormat, leftOp, rightOp);
				if (useBrackets) {
					return String.format("(%s)", result);
				} else {
					return result;
				}
			} else {

				if (functionSymbol == OBDAVocabulary.SPARQL_REGEX) {
					boolean caseinSensitive = false;
					boolean multiLine = false;
					boolean dotAllMode = false;
					if (function.getArity() == 3) {
						if (function.getTerm(2).toString().contains("i")) {
							caseinSensitive = true;
						}
						if (function.getTerm(2).toString().contains("m")) {
							multiLine = true;
						}
						if (function.getTerm(2).toString().contains("s")) {
							dotAllMode = true;
						}
					}
					Term p1 = function.getTerm(0);
					Term p2 = function.getTerm(1);
					
					String column = getSQLString(p1, index, false);
					String pattern = getSQLString(p2, index, false);
					return sqladapter.sqlRegex(column, pattern, caseinSensitive, multiLine, dotAllMode);
				}
				else
					throw new RuntimeException("Cannot translate boolean function: " + functionSymbol);
			}

		} else if (functionSymbol instanceof NumericalOperationPredicate) {
			String expressionFormat = getNumericalOperatorString(functionSymbol);
			String leftOp = getSQLString(term1, index, true);
			Term term2 = function.getTerms().get(1);
			String rightOp = getSQLString(term2, index, true);
			String result = String.format(expressionFormat, leftOp, rightOp);
			if (useBrackets) {
				return String.format("(%s)", result);
			} else {
				return result;
			}

		} else if ((functionSymbol instanceof NonBooleanOperationPredicate)&& (functionSymbol.equals(OBDAVocabulary.SPARQL_LANG)) ) { 
		
			
			Variable var = (Variable) term1;
			Collection<String> posList = index.getColumnReferences(var);
			
			if (posList == null || posList.size() == 0) {
				throw new RuntimeException(
						"Unbound variable found in WHERE clause: " + term);
			}
			
			String langC = posList.iterator().next();
			String langColumn = langC.replaceAll("`$", "Lang`");
			return langColumn;
			
			
			
		}else {
			String functionName = functionSymbol.toString();
			if (functionName.equals(OBDAVocabulary.QUEST_CAST_STR)) {
				String columnName = getSQLString(function.getTerm(0), index,
						false);
				String datatype = ((Constant) function.getTerm(1)).getValue();
				int sqlDatatype = -1;
				if (datatype.equals(OBDAVocabulary.XSD_STRING_URI)) {
					sqlDatatype = Types.VARCHAR;
				}
				if (isStringColType(function, index)) {
					return columnName;
				} else {
					return sqladapter.sqlCast(columnName, sqlDatatype);
				}
			} else if (functionName.equals(OBDAVocabulary.SPARQL_STR_URI)) {
				String columnName = getSQLString(function.getTerm(0), index,
						false);
				if (isStringColType(function, index)) {
					return columnName;
				} else {
					return sqladapter.sqlCast(columnName, Types.VARCHAR);
				}
			} else if (functionName.equals(OBDAVocabulary.SPARQL_COUNT_URI)) {
				if (term1.toString().equals("*")) {
					return "COUNT(*)";
				}
				String columnName = getSQLString(function.getTerm(0), index, false);
				//havingCond = true;
				return "COUNT(" + columnName + ")";
			} else if (functionName.equals(OBDAVocabulary.SPARQL_AVG_URI)) {
				String columnName = getSQLString(function.getTerm(0), index, false);
				//havingCond = true;
				return "AVG(" + columnName + ")";
			} else if (functionName.equals(OBDAVocabulary.SPARQL_SUM_URI)) {
				String columnName = getSQLString(function.getTerm(0), index, false);
				//havingCond = true;
				return "SUM(" + columnName + ")";
			}
		}

		/*
		 * The atom must be of the form uri("...", x, y)
		 */
		String functionName = function.getFunctionSymbol().toString();
		if (functionName.equals(OBDAVocabulary.QUEST_URI)
				|| functionName.equals(OBDAVocabulary.QUEST_BNODE)) {
			return getSQLStringForTemplateFunction(function, index);
		} else {
			throw new RuntimeException("Unexpected function in the query: "
					+ functionSymbol);
		}
	}

}
