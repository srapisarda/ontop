package org.semanticweb.ontop.mongo;

import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BSON;
import org.semanticweb.ontop.model.BNode;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.DataTypePredicate;
import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.OBDADataFactory;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.model.OBDAQueryModifiers;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URIConstant;
import org.semanticweb.ontop.model.URITemplatePredicate;
import org.semanticweb.ontop.model.ValueConstant;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.OBDAQueryModifiers.OrderCondition;
import org.semanticweb.ontop.model.Predicate.COL_TYPE;
import org.semanticweb.ontop.model.impl.DatalogProgramImpl;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.owlrefplatform.core.basicoperations.DatalogNormalizer;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.DB2SQLDialectAdapter;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.JDBCUtility;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.SQLDialectAdapter;
import org.semanticweb.ontop.owlrefplatform.core.sql.SQLGenerator.QueryAliasIndex;
import org.semanticweb.ontop.owlrefplatform.core.srcquerygeneration.NativeQueryGenerator;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.ontop.sql.api.Attribute;
import org.semanticweb.ontop.utils.DatalogDependencyGraphGenerator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class SQLQueryGenerator extends AbstractQueryGenerator implements NativeQueryGenerator {

	private static final String INDENT = "    ";
	
	static final Map<Predicate, String> booleanPredicateToQueryString;
	static {
		Map<Predicate, String> temp = new HashMap<>();
		
		temp.put(OBDAVocabulary.EQ, "%s = %s");
		temp.put(OBDAVocabulary.NEQ, "%s <> %s");
		temp.put(OBDAVocabulary.GT, "%s > %s");
		temp.put(OBDAVocabulary.GTE, "%s >= %s");
		temp.put(OBDAVocabulary.LT, "%s < %s");
		temp.put(OBDAVocabulary.LTE, "%s <= %s");
		temp.put(OBDAVocabulary.AND, "%s AND %s");
		temp.put(OBDAVocabulary.OR, "%s OR %s");
		temp.put(OBDAVocabulary.NOT, "NOT %s");
		temp.put(OBDAVocabulary.IS_NULL, "%s IS NULL");
		temp.put(OBDAVocabulary.IS_NOT_NULL, "%s IS NOT NULL");
		temp.put(OBDAVocabulary.IS_TRUE, "%s IS TRUE");
		temp.put(OBDAVocabulary.SPARQL_LIKE, "%s LIKE %s");
		//we do not need the operator for regex, it should not be used, because the sql adapter will take care of this
		temp.put(OBDAVocabulary.SPARQL_REGEX, ""); 
				
		booleanPredicateToQueryString = ImmutableMap.copyOf(temp);
	}

	static final Map<Predicate, String> arithmeticPredicateToQueryString;
	static {
		Map<Predicate, String> temp = new HashMap<>();
		
		temp.put(OBDAVocabulary.ADD, "%s + %s");
		temp.put(OBDAVocabulary.SUBSTRACT, "%s - %s");
		temp.put(OBDAVocabulary.MULTIPLY, "%s * %s");
						
		arithmeticPredicateToQueryString = ImmutableMap.copyOf(temp);
	}

	static final Map<Predicate, Integer> predicateSQLTypes;
	static final int defaultSQLType = Types.VARCHAR;
	static {
		Map<Predicate, Integer> temp = new HashMap<>();
		temp.put(OBDAVocabulary.XSD_STRING, new Integer(Types.VARCHAR));
		temp.put(OBDAVocabulary.XSD_INTEGER, new Integer(Types.INTEGER));
		temp.put(OBDAVocabulary.XSD_DOUBLE, new Integer(Types.DOUBLE));
		temp.put(OBDAVocabulary.XSD_BOOLEAN, new Integer(Types.BOOLEAN));
		temp.put(OBDAVocabulary.XSD_DECIMAL, new Integer(Types.DECIMAL));
		temp.put(OBDAVocabulary.XSD_DATETIME, new Integer(Types.DATE));		
		temp.put(OBDAVocabulary.RDFS_LITERAL, new Integer(Types.VARCHAR));		
		
		predicateSQLTypes = ImmutableMap.copyOf(temp);
	}

	

	private final JDBCUtility jdbcutil;
	private final SQLDialectAdapter sqladapter;

    private boolean generatingREPLACE = true;

    
    
	public SQLQueryGenerator(DBMetadata metadata, JDBCUtility jdbcutil, SQLDialectAdapter sqladapter) {
		super(metadata);
		this.jdbcutil = jdbcutil;
		this.sqladapter = sqladapter;
	}

	@Override
	public String getBooleanOperatorTemplate(Predicate booleanPredicate) {
		if (booleanPredicateToQueryString.containsKey(booleanPredicate)) {
			return booleanPredicateToQueryString.get(booleanPredicate);
		}

		throw new RuntimeException("Unknown boolean operator: " + booleanPredicate);
	}

	@Override
	public String generateSourceQuery(DatalogProgram query, List<String> signature) throws OBDAException {
		
		DatalogProgram normalizedQuery = normalizeProgram(query);

		if(query.getQueryModifiers().hasModifiers()) {
			return generateQueryWithModifiers(normalizedQuery, signature);
		} else {		
			return generateQuery(normalizedQuery, signature);
		}
	}

	private DatalogProgram normalizeProgram(DatalogProgram program) {
		Set<CQIE> normalizedRules = new HashSet<>();
		for (CQIE rule : program.getRules()) {
			normalizedRules.add(normalizeRule(rule));
		}
		return OBDADataFactoryImpl.getInstance().getDatalogProgram(normalizedRules);
	}

	private CQIE normalizeRule(CQIE rule) {
		CQIE normalizedRule = DatalogNormalizer.foldJoinTrees(rule, true);
		DatalogNormalizer.pullUpNestedReferences(normalizedRule, false);
		DatalogNormalizer.addMinimalEqualityToLeftJoin(normalizedRule);
		return normalizedRule;
	}

	private String generateQuery(DatalogProgram query, List<String> signature) {

		DatalogDependencyGraphGenerator depGraph = new DatalogDependencyGraphGenerator(query);
		Multimap<Predicate, CQIE> ruleIndex = depGraph.getRuleIndex();
		List<Predicate> predicatesInBottomUp = depGraph.getPredicatesInBottomUp();

		// creates and stores information about the query: isDistinct, isOrderBy, 
		// creates view definitions for ans predicates other than ans1
		QueryInfo queryInfo = createQueryInfo(query, depGraph, signature); 

		// This should be ans1, and the rules defining it.
		int size = predicatesInBottomUp.size();
		Predicate predAns1 = predicatesInBottomUp.get(size-1);
		
		Collection<CQIE> ansRules = ruleIndex.get(predAns1);
		List<Predicate> headDataTypes = getHeadDataTypes(ansRules);
		boolean isAns1 = true;
		
		List<String> queryStrings = Lists.newArrayListWithCapacity(ansRules.size());
		for (CQIE cq : ansRules) {
			/* Main loop, constructing the SPJ query for each CQ */

			String querystr = generateQueryFromSingleRule(cq, signature, isAns1, headDataTypes, queryInfo);
			queryStrings.add(querystr);
		}

		return createUnionFromSQLList(queryStrings, queryInfo);
	}

	private QueryInfo createQueryInfo(DatalogProgram query, DatalogDependencyGraphGenerator depGraph, List<String> signature) {
		
		Map<Predicate, String> sqlAnsViewMap = createAnsViews(depGraph, query, signature);
		boolean isDistinct = hasSelectDistinctStatement(query);
		boolean isOrderBy = hasOrderByClause(query);
		
		return new QueryInfo(isDistinct, isOrderBy, sqlAnsViewMap);
	}

	private boolean hasOrderByClause(DatalogProgram query) {
		// TODO Auto-generated method stub
		return false;
	}

	private boolean hasSelectDistinctStatement(DatalogProgram query) {
		// TODO Auto-generated method stub
		return false;
	}

	private List<Predicate> getHeadDataTypes(Collection<CQIE> ansRules) {
		// TODO Auto-generated method stub
		return null;
	}

	private String generateQueryFromSingleRule(CQIE cq, List<String> signature,
			boolean isAns1, List<Predicate> headDataTypes, QueryInfo queryInfo) {
		// TODO Auto-generated method stub
		return null;
	}

	private String createUnionFromSQLList(List<String> queryStrings, QueryInfo queryInfo) {
		Iterator<String> queryStringIterator = queryStrings.iterator();
		StringBuilder result = new StringBuilder();
		if (queryStringIterator.hasNext()) {
			result.append(queryStringIterator.next());
		}

		String UNION = null;
		if (queryInfo.isDistinct()) {
			UNION = "UNION";
		} else {
			UNION = "UNION ALL";
		}
		while (queryStringIterator.hasNext()) {
			result.append("\n");
			result.append(UNION);
			result.append("\n");
			result.append(queryStringIterator.next());
		}
		return result.toString();
	}

	/*
	 * creates a view for every ans predicate in the Datalog input program,
	 * except for ans1
	 */
	private Map<Predicate, String> createAnsViews(
			DatalogDependencyGraphGenerator depGraph, DatalogProgram query,
			List<String> signature) {

		List<Predicate> predicatesInBottomUp = depGraph.getPredicatesInBottomUp();

		List<Predicate> extensionalPredicates = depGraph.getExtensionalPredicates();

		Iterator<Predicate> iterator = predicatesInBottomUp.iterator();
		
		Map<Predicate, String> sqlAnsViewMap = new HashMap<>();
		
		int numPreds = predicatesInBottomUp.size();
		int i = 0;
		while (i < numPreds - 1) {
			Predicate pred = iterator.next();
			if (extensionalPredicates.contains(pred)) {
				/*
				 * extensional predicates are defined by DBs
				 */
			} else {
				boolean isAns1 = false;
				sqlAnsViewMap.put(pred, createViewFrom(pred, metadata, depGraph, query, signature, isAns1));
			}
			i++;
		}
		
		return sqlAnsViewMap;
	}

	private String createViewFrom(Predicate pred, DBMetadata metadata,
			DatalogDependencyGraphGenerator depGraph, DatalogProgram query,
			List<String> signature, boolean isAns1) {
		// TODO Auto-generated method stub
		return null;
	}

	private String generateQueryWithModifiers(DatalogProgram queryProgram, List<String> signature) {
		final String outerViewName = "SUB_QVIEW";
		String subquery = generateQuery(queryProgram, signature);


		List<OrderCondition> conditions = queryProgram.getQueryModifiers().getSortConditions();

		List<Variable> groupby = queryProgram.getQueryModifiers().getGroupConditions();
		// if (!groupby.isEmpty()) {
		// subquery += "\n" + sqladapter.sqlGroupBy(groupby, "") + " " +
		// havingStr + "\n";
		// }
		// List<OrderCondition> conditions =
		// query.getQueryModifiers().getSortConditions();


		String modifier = "";
		if (!conditions.isEmpty()) {
			modifier += sqladapter.sqlOrderBy(conditions, outerViewName)
					+ "\n";
		}
		long limit = queryProgram.getQueryModifiers().getLimit();
		long offset = queryProgram.getQueryModifiers().getOffset();
		if (limit != -1 || offset != -1) {
			modifier += sqladapter.sqlSlice(limit, offset) + "\n";
		}

		String sql = "SELECT *\n";
		sql += "FROM (\n";
		sql += subquery + "\n";
		sql += ") " + outerViewName + "\n";
		sql += modifier;
		return sql;
	}

	@Override
	protected String getAlgebraConditionString(Function atom, QueryVariableIndex index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getArithmeticConditionString(Function atom, QueryVariableIndex index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getDataTypeConditionString(Function atom, QueryVariableIndex index) {
		if (! atom.isDataTypeFunction() ) {
			throw new RuntimeException("Invoked for non-datatype function " + atom.getFunctionSymbol() + "!");
		}
		
		Term term1 = atom.getTerms().get(0);
		//TODO: is the following correct? 
		//in SQL, we do not impose typing constraints in the WHERE clause
		//what about URI? can it appear in the body of the program after unfolding?
		return getNativeString(term1, index);
	}

	
	@Override
	protected String getBooleanConditionString(Function atom, QueryVariableIndex index) {
		if (!atom.isBooleanFunction()) {
			throw new RuntimeException("Invoked for non-Boolean function " + atom.getFunctionSymbol() + "!");
		}

		if (atom.getArity() == 1) {
			return getUnaryBooleanConditionString(atom, index);
			
		} else if (atom.getArity() == 2) {
			return getBinaryBooleanConditionString(atom, index);
		}
		
		if (atom.getFunctionSymbol() == OBDAVocabulary.SPARQL_REGEX) {
			return getRegularExpressionString(atom, index);
		}
		else
			throw new RuntimeException("Cannot translate boolean function: " + atom);

	}

	// for binary functions, e.g., AND, OR, EQ, NEQ, GT, etc.
	private String getBinaryBooleanConditionString(Function atom, QueryVariableIndex index) {
		Predicate booleanPredicate = atom.getFunctionSymbol();
		String expressionFormat = getBooleanOperatorTemplate(booleanPredicate);

		Term term1 = atom.getTerms().get(0);
		Term term2 = atom.getTerms().get(1);
		String leftOp = getNativeString(term1, index);//, true);
		String rightOp = getNativeString(term2, index);//, true);

		String result = String.format(expressionFormat, leftOp, rightOp);
		//if (useBrackets) {
			return String.format("(%s)", result);
		//} else {
		//	return result;
		//}
	}

	// for unary functions, e.g., NOT, IS NULL, IS NOT NULL
	// also added for IS TRUE
	private String getUnaryBooleanConditionString(Function atom, QueryVariableIndex index) {
		Predicate booleanPredicate = atom.getFunctionSymbol();
		String expressionFormat = getBooleanOperatorTemplate(booleanPredicate);

		Term term1 = atom.getTerms().get(0);
		
		if (expressionFormat.contains("IS TRUE")) {
			return getIsTrueString(term1, index);
		}
		
		String op = getNativeString(term1, index);//, true);
		return String.format(expressionFormat, op);
	}

	private String getIsTrueString(Term term1, QueryVariableIndex index) {
		String column = getNativeString(term1, index);
		int type = getVariableDataType(term1);

		// find data type of term and evaluate accordingly
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

	private String getRegularExpressionString(Function atom, QueryVariableIndex index) {
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
		
		String column = getNativeString(p1, index);
		String pattern = getNativeString(p2, index);
		return sqladapter.sqlRegex(column, pattern, caseinSensitive, multiLine, dotAllMode);
	}

	private int getVariableDataType(Term term) {

		if (term instanceof Function){
			Function f = (Function) term;
			if (f.isDataTypeFunction()) {
				Predicate p = f.getFunctionSymbol();
				//TODO: create a method for doing this
				return predicateSQLTypes.containsKey(f) ? 
						predicateSQLTypes.get(f).intValue() : 
							defaultSQLType;
			}
			// Return varchar for unknown
			return defaultSQLType;
		}else if (term instanceof Variable){
			throw new RuntimeException("Cannot return the SQL type for: " + term);
		}
		
		return defaultSQLType;
	}

	private String removeQuotes(String string) {
		while (string.startsWith("\"") && string.endsWith("\"")) {
			string = string.substring(1, string.length() - 1);
		}
		return string;
	}

	
	@Override
	protected String getNativeLexicalForm(URIConstant uc) {
		return jdbcutil.getSQLLexicalForm(uc.getURI());
	}

	@Override
	protected String getNativeLexicalForm(ValueConstant ct) {
		return jdbcutil.getSQLLexicalForm(ct);
	}

}
