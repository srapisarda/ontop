package org.semanticweb.ontop.owlrefplatform.core.sql;

import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;
import org.semanticweb.ontop.mapping.QueryUtils;
import org.semanticweb.ontop.model.AlgebraOperatorPredicate;
import org.semanticweb.ontop.model.BNode;
import org.semanticweb.ontop.model.BooleanOperationPredicate;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.Constant;
import org.semanticweb.ontop.model.DataTypePredicate;
import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.NumericalOperationPredicate;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URIConstant;
import org.semanticweb.ontop.model.URITemplatePredicate;
import org.semanticweb.ontop.model.ValueConstant;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.OBDAQueryModifiers.OrderCondition;
import org.semanticweb.ontop.model.Predicate.COL_TYPE;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.owlrefplatform.core.basicoperations.DatalogNormalizer;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.DB2SQLDialectAdapter;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.HSQLSQLDialectAdapter;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.JDBCUtility;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.SQLDialectAdapter;
import org.semanticweb.ontop.owlrefplatform.core.srcquerygeneration.NativeQueryGenerator;
import org.semanticweb.ontop.owlrefplatform.core.srcquerygeneration.SQLQueryGenerator;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.DataDefinition;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.ontop.sql.ViewDefinition;
import org.semanticweb.ontop.sql.api.Attribute;
import org.semanticweb.ontop.utils.DatalogDependencyGraphGenerator;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

public class MySQLQueryGenerator extends AbstractQueryGenerator implements SQLQueryGenerator {

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
		temp.put(OBDAVocabulary.XSD_INTEGER, new Integer(Types.INTEGER));
		temp.put(OBDAVocabulary.XSD_DOUBLE, new Integer(Types.DOUBLE));
		temp.put(OBDAVocabulary.XSD_BOOLEAN, new Integer(Types.BOOLEAN));
		temp.put(OBDAVocabulary.XSD_DECIMAL, new Integer(Types.DECIMAL));
		temp.put(OBDAVocabulary.XSD_DATETIME, new Integer(Types.DATE));		
		temp.put(OBDAVocabulary.XSD_STRING, new Integer(Types.VARCHAR));
		temp.put(OBDAVocabulary.RDFS_LITERAL, new Integer(Types.VARCHAR));		
		temp.put(OBDAVocabulary.SPARQL_AVG, Types.DECIMAL);
		temp.put(OBDAVocabulary.SPARQL_SUM, Types.DECIMAL);
		temp.put(OBDAVocabulary.SPARQL_COUNT, Types.DECIMAL);
		temp.put(OBDAVocabulary.SPARQL_MAX,Types.DECIMAL);
		temp.put(OBDAVocabulary.SPARQL_MIN, Types.DECIMAL);
		//OBDAVocabulary.XSD_INT_URI: no appropriate predicate for that
		
		predicateSQLTypes = ImmutableMap.copyOf(temp);
	}
		

	private static final Table<Predicate, Predicate, Predicate> dataTypePredicateUnifyTable;
	static{
		dataTypePredicateUnifyTable = new ImmutableTable.Builder<Predicate, Predicate, Predicate>()
			.put(OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.XSD_DOUBLE)
			.put(OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.XSD_DECIMAL)
			.put(OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.XSD_INTEGER)
			.put(OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.OWL_REAL, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.XSD_DECIMAL)
			.put(OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.XSD_DOUBLE)
			.put(OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.OWL_REAL, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.XSD_DECIMAL)
			.put(OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.XSD_DOUBLE)
			.put(OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.XSD_DOUBLE)
			.put(OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.OWL_REAL, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.XSD_DOUBLE)
			.put(OBDAVocabulary.OWL_REAL, OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.OWL_REAL, OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.OWL_REAL, OBDAVocabulary.OWL_REAL, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.OWL_REAL, OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.SPARQL_COUNT, OBDAVocabulary.XSD_DECIMAL, OBDAVocabulary.XSD_DECIMAL)
			.put(OBDAVocabulary.SPARQL_COUNT, OBDAVocabulary.XSD_DOUBLE, OBDAVocabulary.XSD_DOUBLE)
			.put(OBDAVocabulary.SPARQL_COUNT, OBDAVocabulary.OWL_REAL, OBDAVocabulary.OWL_REAL)
			.put(OBDAVocabulary.SPARQL_COUNT, OBDAVocabulary.XSD_INTEGER, OBDAVocabulary.XSD_INTEGER)
			.build();
	}

	static final Map<String, Integer> predicateCodeTypes;
    private static final int UNDEFINED_TYPE_CODE = -1;
	static {
		Map<String, Integer> temp = new HashMap<>();
		temp.put(OBDAVocabulary.XSD_BOOLEAN_URI, new Integer(9));
		temp.put(OBDAVocabulary.XSD_DATETIME_URI, new Integer(8));		
		temp.put(OBDAVocabulary.XSD_STRING_URI, new Integer(7));
		temp.put(OBDAVocabulary.XSD_DOUBLE_URI, new Integer(6));
		temp.put(OBDAVocabulary.XSD_DECIMAL_URI, new Integer(5));
		temp.put(OBDAVocabulary.XSD_INTEGER_URI, new Integer(4));
		temp.put(OBDAVocabulary.RDFS_LITERAL_URI, new Integer(3));
		temp.put(OBDAVocabulary.QUEST_BNODE, 2);
     	temp.put(OBDAVocabulary.QUEST_URI, 1);
		
		predicateCodeTypes = ImmutableMap.copyOf(temp);
	}
        
	/**
	 * Formatting template
	 */
    private static final String TYPE_STR = "%s AS \"%sQuestType\"" ;
	static final String VIEW_NAME = "Q%sVIEW%s";
	static final String VIEW_ANS_NAME = "Q%sView";

	private final JDBCUtility jdbcutil;
	private final SQLDialectAdapter sqladapter;
	private final String QUEST_TYPE = "QuestType";

    private boolean generatingREPLACE = true;

    
    
	public MySQLQueryGenerator(DBMetadata metadata, JDBCUtility jdbcutil, SQLDialectAdapter sqladapter) {
		super(metadata);
		this.jdbcutil = jdbcutil;
		this.sqladapter = sqladapter;
	}

	@Override
	public SQLQueryGenerator cloneGenerator() {
	    return new MySQLQueryGenerator(metadata.clone(), jdbcutil, sqladapter);
	}

	@Override
	public String getBooleanOperatorTemplate(Predicate booleanPredicate) {
		if (booleanPredicateToQueryString.containsKey(booleanPredicate)) {
			return booleanPredicateToQueryString.get(booleanPredicate);
		}

		throw new RuntimeException("Unknown boolean operator: " + booleanPredicate);
	}
	
	@Override
	public String getArithmeticOperatorString(Predicate arithmeticPredicate) {
		if (arithmeticPredicateToQueryString.containsKey(arithmeticPredicate)) {
			return arithmeticPredicateToQueryString.get(arithmeticPredicate);
		}

		throw new RuntimeException("Unknown arithmetic operator: " + arithmeticPredicate);
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

	private String generateQueryWithModifiers(DatalogProgram queryProgram, List<String> signature) {

		final String outerViewName = "SUB_QVIEW";

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

		String subquery = generateQuery(queryProgram, signature);

		String sql = "SELECT *\n";
		sql += "FROM (\n";
		sql += subquery + "\n";
		sql += ") " + outerViewName + "\n";
		sql += modifier;
		return sql;
	}

	private String generateQuery(DatalogProgram query, List<String> signature) {

		DatalogDependencyGraphGenerator depGraph = new DatalogDependencyGraphGenerator(query);
		Multimap<Predicate, CQIE> ruleIndex = depGraph.getRuleIndex();
		List<Predicate> predicatesInBottomUp = depGraph.getPredicatesInBottomUp();

		// creates and stores information about the query: isDistinct, isOrderBy, 
		// creates view definitions for ans predicates other than ans1
		QueryInfo queryInfo = createQueryInfo(query, depGraph); 

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

	private QueryInfo createQueryInfo(DatalogProgram query, DatalogDependencyGraphGenerator depGraph) {
		
		boolean isDistinct = hasSelectDistinctStatement(query);
		boolean isOrderBy = hasOrderByClause(query);
		Map<Predicate, String> map = new HashMap<>();
		QueryInfo queryInfo = new QueryInfo(isDistinct, isOrderBy, map);
		
		return createAnsViews(depGraph, query, queryInfo);
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
	private QueryInfo createAnsViews(DatalogDependencyGraphGenerator depGraph, DatalogProgram query,
									QueryInfo queryInfo) {

		List<Predicate> predicatesInBottomUp = depGraph.getPredicatesInBottomUp();

		List<Predicate> extensionalPredicates = depGraph.getExtensionalPredicates();

		Iterator<Predicate> iterator = predicatesInBottomUp.iterator();
		
		QueryInfo currentQueryInfo = queryInfo;
		
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
				currentQueryInfo = createViewFrom(pred, depGraph.getRuleIndex().get(pred), isAns1, currentQueryInfo);
			}
			i++;
		}
		
		return currentQueryInfo;
	}

	private QueryInfo createViewFrom(Predicate pred, Collection<CQIE> ruleList, boolean isAns1, QueryInfo queryInfo) {
		/* Creates BODY of the view query */

		List<Predicate> headDataTypes = getHeadDataTypes(ruleList);
		int headArity = 0;
		
		List<String> sqls = Lists.newArrayListWithExpectedSize(ruleList.size());
		for (CQIE rule : ruleList) {
			Function cqHead = rule.getHead();

			// FIXME: the arity of the predicate might be wrong, should be fixed
			// in the unfolder
			// headArity = cqHead.getArity();
			headArity = cqHead.getTerms().size();

			List<String> varContainer = QueryUtils.getVariableNamesInAtom(cqHead);

			/* Creates the SQL for the View */
			String sqlQuery = generateQueryFromSingleRule(rule, varContainer, isAns1, headDataTypes, queryInfo);
			sqls.add(sqlQuery);
		}

		String viewname = String.format(VIEW_ANS_NAME, pred);

		String unionView;
		if (sqls.size() == 1) {
			unionView = sqls.iterator().next();
		} else {
			unionView = "(" + Joiner.on(")\n UNION \n (").join(sqls) + ")";
		}

		// Hard coded variable names
		List<String> columns = Lists.newArrayListWithExpectedSize(3 * headArity);
		for (int i = 0; i < headArity; i++) {
			columns.add("v" + i + QUEST_TYPE);
			columns.add("v" + i + "lang");
			columns.add("v" + i);
		}

		/* Creates the View itself */
		ViewDefinition viewU = metadata.createViewDefinition(viewname, unionView, columns);
		QueryInfo newQueryInfo = QueryInfo.addViewDefinition(queryInfo, viewU);
		newQueryInfo =  QueryInfo.addAnsView(newQueryInfo, pred, unionView);
		return newQueryInfo;
	}

	private String generateQueryFromSingleRule(CQIE cq, List<String> signature,
			boolean isAns1, List<Predicate> headDataTypes, QueryInfo queryInfo) {
		SQLQueryVariableIndex index = new SQLQueryVariableIndex(cq, metadata, queryInfo, sqladapter);

		String SELECT = getSelectClause(signature, cq, index, isAns1, headDataTypes, queryInfo);
		String FROM = getFROM(cq.getBody(), index);
		String WHERE = getWHERE(cq.getBody(), index);
		String GROUP = getGroupBy(cq.getBody(), index);
		String HAVING = getHaving(cq.getBody(), index);;
		
		String querystr = SELECT + FROM + WHERE + GROUP + HAVING;
		return querystr;
	}

	private String getGroupBy(List<Function> body, SQLQueryVariableIndex index) {

		List<Variable> varsInGroupBy = Lists.newArrayList();
		for (Function atom : body) {
			if (atom.getFunctionSymbol().equals(OBDAVocabulary.SPARQL_GROUP)) {
				varsInGroupBy.addAll(QueryUtils.getVariablesInAtom(atom));
			}
		}

		List<String> groupReferences = Lists.newArrayList();
		for (Variable var : varsInGroupBy) {
			Collection<String> references = index.getColumnReferences(var);
			groupReferences.addAll(references);
		}

		StringBuilder result = new StringBuilder();
		if (!groupReferences.isEmpty()) {
			result.append(" GROUP BY ");
			Joiner.on(" , ").appendTo(result, groupReferences);
		}

		return result.toString();
	}

	private String getHaving(List<Function> body, QueryVariableIndex index) {
		List <Term> conditions = new LinkedList<Term> ();
		List <Function> condFunctions = new LinkedList<Function> ();

		for (Function atom : body) {
			if (atom.getFunctionSymbol().equals(OBDAVocabulary.SPARQL_HAVING)) {
				conditions = atom.getTerms();
				break;
			}
		}
		if (conditions.isEmpty()) {
			return "";
		}

		for(Term cond : conditions){
			condFunctions.add((Function) cond);
		}
		
		Set<String> condSet = getConditionsString(condFunctions, index);
		
		StringBuilder result = new StringBuilder();
		result.append(" HAVING ( ");
		for (String c: condSet) {
			result.append(c);
		} 
		result.append(" ) ");
		return result.toString();
	}
	
	private String getSelectClause(List<String> signature, CQIE query,
			SQLQueryVariableIndex index, boolean isAns1,
			List<Predicate> headDataTypes, QueryInfo queryInfo) {
		/*
		 * If the head has size 0 this is a boolean query.
		 */
		List<Term> headterms = query.getHead().getTerms();
		StringBuilder selectString = new StringBuilder();

		selectString.append("SELECT ");
		if (queryInfo.isDistinct()) {
			selectString.append("DISTINCT ");
		}
		//Only for ASK
		if (headterms.size() == 0) {
			selectString.append("'true' as x");
			return selectString.toString();
		}

		Iterator<Term> headTermIter = headterms.iterator();
		Iterator<Predicate> headDataTypeIter = headDataTypes.iterator();

		int headPos = 0;
		while (headTermIter.hasNext()) {
			Term headTerm = headTermIter.next();
			Predicate headDataType = headDataTypeIter.next();
			
			/*
			 * When isAns1 is true, we need to use the <code>signature</code>
			 * for the varName
			 */
			String varName;
			if (isAns1) {
				varName = signature.get(headPos);
			} else {
				varName = "v" + headPos;
			}

			String typeColumn = getTypeColumnForSELECT(headTerm, varName, index);
			String mainColumn = getMainColumnForSELECT(headTerm, varName, index, headDataType, queryInfo);
			String langColumn = getLangColumnForSELECT(headTerm, varName, index);

			selectString.append("\n   ");
			selectString.append(typeColumn).append(", ");
			selectString.append(langColumn).append(", ");
			selectString.append(mainColumn);
			if (headTermIter.hasNext()) {
				selectString.append(", ");
			}

			headPos++;
		}
		return selectString.toString();
	}

    /**

     * Infers the type of a projected term.
     *
     * @param projectedTerm
     * @param varName Name of the variable
     * @param index Used when the term correspond to a column name
     * @return A string like "5 AS ageQuestType"
     */
	private String getTypeColumnForSELECT(Term projectedTerm, String varName,
			SQLQueryVariableIndex index) {

		if (projectedTerm instanceof Function) {
			return getCompositeTermType((Function) projectedTerm, varName);
		}
        else if (projectedTerm instanceof URIConstant) {
			return String.format(TYPE_STR, 1, varName);
		}
        else if (projectedTerm == OBDAVocabulary.NULL) {
			return String.format(TYPE_STR, 0, varName);
		}
        else if (projectedTerm instanceof Variable) {
			return getTypeFromVariable((Variable) projectedTerm, index, varName);
		}

        // Unusual term
		throw new RuntimeException("Cannot generate SELECT for term: "
				+ projectedTerm.toString());

	}
	
    /**
     * Gets the type expression for a composite term.
     *
     * There is two common form of composite terms considered here:
     *   1. Typed variable. For instance, "http://...#decimal(cost)"
     *         should return something like "5 AS costQuestType"
     *   2. Aggregation. For instance, "SUM(http://...#decimal(cost))"
     *         should return something like "5 AS totalCostQuestType"
     *
     *   Basically, it tries to infer the type by looking at function symbols.
     *
     */
    private String getCompositeTermType(Function compositeTerm, String varName) {
        Predicate mainFunctionSymbol = compositeTerm.getFunctionSymbol();

        int typeCode = UNDEFINED_TYPE_CODE;

        switch(mainFunctionSymbol.getName()) {
            /**
             * Aggregation cases
             */
            case OBDAVocabulary.SPARQL_COUNT_URI:
                typeCode = predicateCodeTypes.get(OBDAVocabulary.XSD_INTEGER);
                break;

            case OBDAVocabulary.SPARQL_SUM_URI:
            case OBDAVocabulary.SPARQL_AVG_URI:
            case OBDAVocabulary.SPARQL_MIN_URI:
            case OBDAVocabulary.SPARQL_MAX_URI:

                // We look at the sub-term
                Term subTerm = compositeTerm.getTerm(0);
                if (subTerm instanceof Function) {
                    Function compositeSubTerm = (Function) subTerm;

                    typeCode = getCodeTypeFromFunctionSymbol(compositeSubTerm.getFunctionSymbol());
                }

                /**
                 * Sometimes we cannot infer the type by looking at the term.
                 *
                 * In such a case, we cast the aggregate to a xsd:double
                 * (any number can be promoted to a double http://www.w3.org/TR/xpath20/#promotion) .
                 */
                if (typeCode == UNDEFINED_TYPE_CODE) {
                    typeCode = predicateCodeTypes.get(OBDAVocabulary.XSD_DOUBLE);
                }
                break;

            /**
             * Not a (known) aggregation function symbol
             */
            default:
                typeCode = getCodeTypeFromFunctionSymbol(mainFunctionSymbol);
                if (typeCode == UNDEFINED_TYPE_CODE) {
                    throw new RuntimeException("Cannot generate the SQL query " +
                            "because of an untyped term: " + compositeTerm.toString());
                }
        }

        return String.format(TYPE_STR, typeCode, varName);
    }

    /**
     * Converts a function symbol into a code type (integer).
     *
     * May return an UNDEFINED_TYPE_CODE value.
     *
     * @param predicate
     * @return
     */
    private int getCodeTypeFromFunctionSymbol(Predicate predicate) {
    	String predName = predicate.getName();
        return predicateCodeTypes.containsKey(predName) ? predicateCodeTypes.get(predName) : UNDEFINED_TYPE_CODE;
    }

    /**
     * Gets the type of a variable.
     *
     * Such variable does not hold this information, so we have to look
     * at the database metadata.
     *
     *
     * @param var
     * @param index
     * @return
     */
    private String getTypeFromVariable(Variable var, SQLQueryVariableIndex index, String varName) {
        Collection<String> columnRefs = index.getColumnReferences(var);

        if (columnRefs == null || columnRefs.size() == 0) {
            throw new RuntimeException(
                    "Unbound variable found in WHERE clause: " + var);
        }

        /**
         * By default, we assume that the variable is an IRI.
         *
         * TODO: why?
         * TODO: do not use such a "magical" number.
         */
        String typeCode = "1";

        /**
         * For each column reference corresponding to the variable.
         *
         * For instance, columnRef is `Qans4View`.`v1` .
         */
        for (String columnRef : columnRefs) {
            String columnType, tableColumnType;

            String[] splits = columnRef.split("\\.");

            String quotedTable = splits[0];
            String table = removeQuotes(splits[0]);
            String column = removeQuotes(splits[1]);

            DataDefinition definition = metadata.getDefinition(table);
            /**
             * If the var is defined in a ViewDefinition, then there is a
             * column for the type and we just need to refer to that column.
             *
             * For instance, tableColumnType becomes `Qans4View`.`v1QuestType` .
             */
            if (definition instanceof ViewDefinition) {
                columnType = column + QUEST_TYPE;
                tableColumnType = sqladapter.sqlQualifiedColumn(quotedTable, columnType);
                typeCode = tableColumnType ;
                break;
            }
        }

        return String.format(TYPE_STR, typeCode, varName);
    }

	private String getMainColumnForSELECT(Term headTerm, String varName, SQLQueryVariableIndex index, Predicate typePredicate, QueryInfo queryInfo) {

		String mainColumn = null;
		String mainTemplate = "%s AS %s";

		if (headTerm instanceof URIConstant || headTerm instanceof Variable) {
			mainColumn = getNativeString(headTerm, index);
		} else if (headTerm == OBDAVocabulary.NULL) {
			mainColumn = "NULL";
		} else if (headTerm instanceof Function) {
			/*
			 * if it's a function we need to get the nested value if its a
			 * datatype function, or we need to do the CONCAT if its URI(....).
			 */
			Function atom = (Function) headTerm;
			Predicate predicate = atom.getFunctionSymbol();
			String predicateString = predicate.toString();

			/*
			 * Adding the column(s) with the actual value(s)
			 */
			if (predicateString.equals(OBDAVocabulary.QUEST_URI) ||
					predicateString.equals(OBDAVocabulary.QUEST_BNODE)) {
				/*
				 * New template based URI or BNODE building functions
				 */
				mainColumn = getSQLStringForTemplateFunction(atom, index, queryInfo);
			
			} 
			
			else if (predicate instanceof DataTypePredicate) {
				/*
				 * Case where we have a typing function in the head (this is the
				 * case for all literal columns
				 */
				//TODO can we simplify here? Why Literal? Not RDFS_Literal?
				String termStr = null;
				if ((predicate instanceof Literal) || atom.getTerms().size() > 2) {
					termStr = getSQLStringForTemplateFunction(atom, index, queryInfo);
				} else {
					Term term = atom.getTerm(0);
					termStr = getNativeString(term, index);
				}
				mainColumn = termStr;

			}
			
			// Aggregates
			else if (predicateString.equals(OBDAVocabulary.SPARQL_COUNT_URI) ||
					predicateString.equals(OBDAVocabulary.SPARQL_SUM_URI) ||
					predicateString.equals(OBDAVocabulary.SPARQL_AVG_URI) || 
					predicateString.equals(OBDAVocabulary.SPARQL_MIN_URI) ||
					predicateString.equals(OBDAVocabulary.SPARQL_MAX_URI)) {
				mainColumn = predicateString.toUpperCase() + "("+ getNativeString(atom.getTerm(0), index) + ")";
		
			}
			
			else {
				throw new IllegalArgumentException(
						"Error generating SQL query. Found an invalid function during translation: "
								+ atom.toString());
			}
		} else {
			throw new RuntimeException("Cannot generate SELECT for term: "
					+ headTerm.toString());
		}

		/*
		 * If the we have a column we need to still CAST to VARCHAR
		 */
		if (mainColumn.charAt(0) != '\'' && mainColumn.charAt(0) != '(') {

			if (typePredicate != null){
				
				int sqlType = getNativeType(typePredicate);				
				mainColumn = sqladapter.sqlCast(mainColumn, sqlType);
			}
			
		}
				
		String format = String.format(mainTemplate, mainColumn, sqladapter.sqlQuote(varName));
		return format;
	}

	/**
	 * this method is used for URI, BNODE, Literal predicates, and 
	 * for Datatype predicates with arity > 2
	 *  
	 * @param atom
	 * @param index
	 * @param queryInfo 
	 * @return
	 */
	public String getSQLStringForTemplateFunction(Function atom, SQLQueryVariableIndex index, QueryInfo queryInfo) {
		/*
		 * The first inner term determines the form of the result
		 */
		Term term1 = atom.getTerm(0);

		if (term1 instanceof ValueConstant || term1 instanceof BNode) {
			/*
			 * The function is actually a template. The first parameter is a
			 * string of the form http://.../.../ or empty "{}" with place
			 * holders of the form {}. The rest are variables or constants that
			 * should be put in place of the place holders. We need to tokenize
			 * and form the CONCAT
			 */
			return getConcatFromTemplate(atom, term1, index, queryInfo);
			
		} else if (term1 instanceof Variable) {
			/*
			 * The function is of the form uri(x), we need to simply return the
			 * value of X
			 */
			return getNativeString(term1, index);

		} else if (term1 instanceof URIConstant) {
			/*
			 * The function is of the form uri("http://some.uri/"), i.e., a
			 * concrete URI, we return the string representing that URI.
			 */
			return getNativeString(term1, index);
		}

		/*
		 * Unsupported case
		 */
		throw new IllegalArgumentException(
				"Error, cannot generate URI constructor clause for a term: "
						+ atom.toString());

	}


	private String getConcatFromTemplate(Function atom, Term term1, SQLQueryVariableIndex index, QueryInfo queryInfo) {
		String literalValue = "";
		if (term1 instanceof BNode) {
            literalValue = ((BNode) term1).getValue();
		} else {
            literalValue = ((ValueConstant) term1).getValue();
		}
		
		Predicate pred = atom.getFunctionSymbol();


		String replace1;
        String replace2;
        if(generatingREPLACE) {

            replace1 = "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(" +
                    "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(";

            replace2 = ",' ', '%20')," +
                    "'!', '%21')," +
                    "'@', '%40')," +
                    "'#', '%23')," +
                    "'$', '%24')," +
                    "'&', '%26')," +
                    "'*', '%42'), " +
                    "'(', '%28'), " +
                    "')', '%29'), " +
                    "'[', '%5B'), " +
                    "']', '%5D'), " +
                    "',', '%2C'), " +
                    "';', '%3B'), " +
                    "':', '%3A'), " +
                    "'?', '%3F'), " +
                    "'=', '%3D'), " +
                    "'+', '%2B'), " +
                    "'''', '%22'), " +
                    "'/', '%2F')";
        } else {
            replace1 = replace2 = "";
        }

        String template = removeAllQuotes(literalValue);

		String[] split = template.split("[{][}]");

		List<String> vex = new LinkedList<String>();
		if (split.length > 0 && !split[0].isEmpty()) {
			vex.add(jdbcutil.getSQLLexicalForm(split[0]));
		}

		/*
		 * New we concat the rest of the function, note that if there is
		 * only 1 element there is nothing to concatenate
		 */
		if (atom.getTerms().size() > 1) {
			int size = atom.getTerms().size();
			if (pred.equals(OBDAVocabulary.RDFS_LITERAL)
					|| pred.equals(OBDAVocabulary.RDFS_LITERAL_LANG)) {
				size--;
			}
			for (int termIndex = 1; termIndex < size; termIndex++) {
				Term currentTerm = atom.getTerms().get(termIndex);
				String repl = "";
				if (isStringColType(currentTerm, index)) {
					repl = replace1
							+ getNativeString(currentTerm, index)
							+ replace2;
				} else {
					repl = replace1
							+ sqladapter.sqlCast(getNativeString(currentTerm, index), Types.VARCHAR) 
							+ replace2;
				}
				vex.add(repl);
				if (termIndex < split.length) {
					vex.add(jdbcutil.getSQLLexicalForm(split[termIndex]));
				}
			}
		}

		if (vex.size() == 1) {
			return vex.get(0);
		}
		
		String[] params = new String[vex.size()];
		int i = 0;
		for (String param : vex) {
			params[i] = param;
			i++;
		}
		return MySQLQueryGenerator.getStringConcatenation(sqladapter, params, queryInfo);
	}

	private boolean isStringColType(Term term, SQLQueryVariableIndex index) {
		/*
		 * term is a Function
		 */
		if (term instanceof Function) {
			Function function = (Function) term;
			Predicate predicate = function.getFunctionSymbol();
			
			if (predicate instanceof URITemplatePredicate) {
				/*
				 * A URI function always returns a string, thus it is a string
				 * column type.
				 */
				//if (isSI)
				//	return false;
				return true;
			}
			
			else {
				if (function.getTerms().size() == 1) {
					if (predicate.getName().equals(OBDAVocabulary.SPARQL_COUNT_URI)) {
						return false;
					}
					/*
					 * Update the term with the parent term's first parameter.
					 * Note: this method is confusing :(
					 */
					Term term1 = function.getTerm(0);
					return isStringColType(term1, index);
				}
			}
		}
		/*
		 * term is a Variable
		 */
		else if (term instanceof Variable) {
		 
			Collection<String> viewdef = index.getColumnReferences((Variable) term);
			String def = viewdef.iterator().next();
			String col = removeAllQuotes(def.split("\\.")[1]);
			String table = def.split("\\.")[0];
			if (def.startsWith("QVIEW")) {
				Map<Function, String> views = index.getViewNames();
				for (Function func : views.keySet()) {
					String value = views.get(func);
					if (value.equals(def.split("\\.")[0])) {
						table = func.getFunctionSymbol().toString();
						break;
					}
				}
			}
			List<TableDefinition> tables = metadata.getTableList();
			for (TableDefinition tabledef : tables) {
				if (tabledef.getName().equals(table)) {
					List<Attribute> attr = tabledef.getAttributes();
					for (Attribute a : attr) {
						if (a.getName().equals(col)) {
							switch (a.getType()) {
							case Types.VARCHAR:
							case Types.CHAR:
							case Types.LONGNVARCHAR:
							case Types.LONGVARCHAR:
							case Types.NVARCHAR:
							case Types.NCHAR:
								return true;
							default:
								return false;
							}
						}
					}
				}
			}
		}
		return false;
	}

    /**
    * Adding the ColType column to the projection (used in the result
    * set to know the type of constant)
    */
	private String getLangColumnForSELECT(Term headTerm, String varName, QueryVariableIndex index) {

		String langStr = "%s AS \"%sLang\"";

		// String varName = signature.get(hpos);
		if (headTerm instanceof Function) {
			Function atom = (Function) headTerm;
			Predicate function = atom.getFunctionSymbol();

			if (function.equals(OBDAVocabulary.RDFS_LITERAL)
					|| function.equals(OBDAVocabulary.RDFS_LITERAL_LANG)) {
				if (atom.getTerms().size() > 1) {
					/*
					 * Case for rdf:literal s with a language, we need to select
					 * 2 terms from ".., rdf:literal(?x,"en"),
					 * 
					 * and signature "name" * we will generate a select with the
					 * projection of 2 columns
					 * 
					 * , 'en' as nameqlang, view.colforx as name,
					 */
					String lang = null;
					int last = atom.getTerms().size() - 1;
					Term langTerm = atom.getTerms().get(last);
					if (langTerm == OBDAVocabulary.NULL) {

						if (sqladapter instanceof HSQLSQLDialectAdapter) {
							lang = "CAST(NULL AS VARCHAR(3))";
						} else {
							lang = "NULL";
						}

					} else {
						lang = getNativeString(langTerm, index);
					}
					return (String.format(langStr, lang, varName));
				}
			}
		}


		if (sqladapter instanceof HSQLSQLDialectAdapter) {
			return (String.format(langStr, "CAST(NULL AS VARCHAR(3))", varName));
		} 
		return (String.format(langStr,  "NULL", varName));
	}

	private String getFROM(List<Function> atoms, SQLQueryVariableIndex index) {
		String tableDefinitions = getTableDefinitions(atoms, index, true, false, "");
		return "\n FROM \n" + tableDefinitions;
	}

	/**
	 * Returns the table definition for these atoms. By default, a list of atoms
	 * represents JOIN or LEFT JOIN of all the atoms, left to right. All boolean
	 * atoms in the list are considered conditions in the ON clause of the JOIN.
	 * 
	 * <p>
	 * If the list is a LeftJoin, then it can only have 2 data atoms, and it HAS
	 * to have 2 data atoms.
	 * 
	 * <p>
	 * If process boolean operators is enabled, all boolean conditions will be
	 * added to the ON clause of the first JOIN.
	 * 
	 * @param inneratoms
	 * @param index
	 * @param isTopLevel
	 *            indicates if the list of atoms is actually the main body of
	 *            the conjunctive query. If it is, no JOIN is generated, but a
	 *            cross product with WHERE clause. Moreover, the isLeftJoin
	 *            argument will be ignored.
	 * 
	 * @return
	 */
	private String getTableDefinitions(List<Function> inneratoms,
			SQLQueryVariableIndex index, boolean isTopLevel, boolean isLeftJoin,
			String indent) {
		/*
		 * We now collect the view definitions for each data atom each
		 * condition, and each each nested Join/LeftJoin
		 */
		List<String> tableDefinitions = new LinkedList<String>();
		for (int atomidx = 0; atomidx < inneratoms.size(); atomidx++) {
			Term innerAtom = inneratoms.get(atomidx);
			Function innerAtomAsFunction = (Function) innerAtom;
			String indent2 = indent + INDENT;
			String definition = getTableDefinition(innerAtomAsFunction, index, indent2);
			if (!definition.isEmpty()) {
				tableDefinitions.add(definition);
			}
		}

		/*
		 * Now we generate the table definition, this will be either a comma
		 * separated list for TOP level (FROM clause) or a Join/LeftJoin
		 * (possibly nested if there are more than 2 table definitions in the
		 * current list) in case this method was called recursively.
		 */
		StringBuilder tableDefinitionsString = new StringBuilder();

		int size = tableDefinitions.size();
		if (isTopLevel) {
			if (size == 0) {
				tableDefinitionsString.append("(" + jdbcutil.getDummyTable() + ") tdummy ");

			} else {
				Iterator<String> tableDefinitionsIterator = tableDefinitions.iterator();
				tableDefinitionsString.append(indent);
				tableDefinitionsString.append(tableDefinitionsIterator.next());
				while (tableDefinitionsIterator.hasNext()) {
					tableDefinitionsString.append(",\n");
					tableDefinitionsString.append(indent);
					tableDefinitionsString.append(tableDefinitionsIterator.next());
				}
			}
		} else {
			/*
			 * This is actually a Join or LeftJoin, so we form the JOINs/LEFT
			 * JOINs and the ON clauses
			 */
			String JOIN_KEYWORD = null;
			if (isLeftJoin) {
				JOIN_KEYWORD = "LEFT OUTER JOIN";
			} else {
				JOIN_KEYWORD = "JOIN";
			}
			
//			String JOIN = "\n" + indent + "(\n" + indent + "%s\n" + indent
//					+ JOIN_KEYWORD + "\n" + indent + "%s\n" + indent + ")";

			String JOIN = "" + indent + "" + indent + "%s\n" + indent
					+ JOIN_KEYWORD + "\n" + indent + "%s" + indent + "";
			
			
			if (size == 0) {
				throw new RuntimeException(
						"Cannot generate definition for empty data");
			}
			if (size == 1) {
				return tableDefinitions.get(0);
			}

			/*
			 * To form the JOIN we will cycle through each data definition,
			 * nesting the JOINs as we go. The conditions in the ON clause will
			 * go on the TOP level only.
			 */
			String currentJoin = String.format(JOIN,
					tableDefinitions.get(size - 2),
					tableDefinitions.get(size - 1));
			tableDefinitions.remove(size - 1);
			tableDefinitions.remove(size - 2);

			int currentSize = tableDefinitions.size();
			while (currentSize > 0) {
				currentJoin = String.format(JOIN,
						tableDefinitions.get(currentSize - 1), currentJoin);
				tableDefinitions.remove(currentSize - 1);
				currentSize = tableDefinitions.size();
			}
			tableDefinitions.add(currentJoin);

			tableDefinitionsString.append(currentJoin);
			/*
			 * If there are ON conditions we add them now. We need to remove the
			 * last parenthesis ')' and replace it with ' ON %s)' where %s are
			 * all the conditions
			 */
			Set<String> conditions = getConditionsString(inneratoms, index);

			String ON_CLAUSE = String.format(" ON\n%s\n " + indent, getConjunctionOfConditions(conditions));
			tableDefinitionsString.append(ON_CLAUSE);
		}
		return tableDefinitionsString.toString();
	}

	private String getConjunctionOfConditions(Collection<String> conditions) {
		return Joiner.on(" , ").join(conditions);
	}

	/**
	 * Returns the table definition for the given atom. If the atom is a simple
	 * table or view, then it returns the value as defined by the
	 * QueryAliasIndex. If the atom is a Join or Left Join, it will call
	 * getTableDefinitions on the nested term list.
	 */
	private String getTableDefinition(Function atom, SQLQueryVariableIndex index, String indent) {
		Predicate predicate = atom.getFunctionSymbol();
		if (predicate instanceof BooleanOperationPredicate
				|| predicate instanceof NumericalOperationPredicate
				|| predicate instanceof DataTypePredicate) {
			// These don't participate in the FROM clause
			return "";
		} 
		
		else if (predicate instanceof AlgebraOperatorPredicate) {
			if (predicate.getName().equals("Group")) {
				return "";
			}
			List<Function> innerTerms = new LinkedList<Function>();
			for (Term innerTerm : atom.getTerms()) {
				innerTerms.add((Function) innerTerm);
			}
			if (predicate == OBDAVocabulary.SPARQL_JOIN) {
				String indent2 = indent + INDENT;
				String tableDefinitions = getTableDefinitions(innerTerms, index, false, false, indent2);
				return tableDefinitions;
			} else if (predicate == OBDAVocabulary.SPARQL_LEFTJOIN) {

				return getTableDefinitions(innerTerms, index, false, true, indent + INDENT);
			}
		}

		/*
		 * This is a data atom
		 */
		String def = index.getViewDefinition(atom);
		return def;
	}

	private String getWHERE(List<Function> atoms, QueryVariableIndex index) {
		Set<String> conditions = getConditionsString(atoms, index);
		if (conditions.isEmpty()) {
			return "";
		}
		
		StringBuilder conditionsString = new StringBuilder();
		Iterator<String> conditionsIterator = conditions.iterator();
		if (conditionsIterator.hasNext()) {
			conditionsString.append(conditionsIterator.next());
		}
		while (conditionsIterator.hasNext()) {
			conditionsString.append(" AND\n");
			conditionsString.append(conditionsIterator.next());
		}
	
		return "\nWHERE \n" + conditionsString.toString();
	}

	private boolean hasOrderByClause(DatalogProgram query) {
		boolean toReturn = false;
		if (query.getQueryModifiers().hasModifiers()) {
			final List<OrderCondition> conditions = query.getQueryModifiers().getSortConditions();
			toReturn = (!conditions.isEmpty());
		}
		return toReturn;
	}

	private boolean hasSelectDistinctStatement(DatalogProgram query) {
		boolean toReturn = false;
		if (query.getQueryModifiers().hasModifiers()) {
			toReturn = query.getQueryModifiers().isDistinct();
		}
		return toReturn;
	}

	private List<Predicate> getHeadDataTypes(Collection<CQIE> rules) {
		int ansArtiy = rules.iterator().next().getHead().getTerms().size();

		List<Predicate> ansTypes = Lists.newArrayListWithCapacity(ansArtiy);
		for (int k = 0; k < ansArtiy; k++) {
			ansTypes.add(null);
		}

		for (CQIE rule : rules) {
			Function head = rule.getHead();
			List<Term> terms = head.getTerms();
			for (int j = 0; j < terms.size(); j++) {
				Term term = terms.get(j);
				getHeadTermDataType(term, ansTypes, j);
			}
		}
		return ansTypes;
	}

    private void getHeadTermDataType(Term term, List<Predicate> ansTypes, int j) {
    	if (term instanceof Function) {
			Function f = (Function) term;
			Predicate typePred = f.getFunctionSymbol();

			if (typePred.isDataTypePredicate()
					|| typePred.getName().equals(OBDAVocabulary.QUEST_URI)) {
				Predicate unifiedType = unifyTypes(ansTypes.get(j), typePred);
				ansTypes.set(j, unifiedType);

			} else if (typePred.getName().equals(OBDAVocabulary.QUEST_BNODE)) {
				ansTypes.set(j, OBDAVocabulary.XSD_STRING);

			} else if ((typePred.getName().equals(OBDAVocabulary.SPARQL_AVG_URI))
					|| (typePred.getName().equals(OBDAVocabulary.SPARQL_SUM_URI))
					|| (typePred.getName().equals(OBDAVocabulary.SPARQL_MAX_URI))
					|| (typePred.getName().equals(OBDAVocabulary.SPARQL_MIN_URI))) {

				Term agTerm = f.getTerm(0);
				if (agTerm instanceof Function) {
					Function agFunc = (Function) agTerm;
					typePred = agFunc.getFunctionSymbol();
					Predicate unifiedType = unifyTypes(ansTypes.get(j), typePred);
					ansTypes.set(j, unifiedType);

				} else {
					Predicate unifiedType = unifyTypes(ansTypes.get(j), OBDAVocabulary.XSD_DECIMAL);
					ansTypes.set(j, unifiedType);
				}
			} else {
				throw new IllegalArgumentException();
			}

		} else if (term instanceof Variable) {
			// FIXME: properly handle the types by checking the metadata
			ansTypes.set(j, OBDAVocabulary.XSD_STRING);
		} else if (term instanceof ValueConstant) {
			COL_TYPE type = ((ValueConstant) term).getType();
			Predicate typePredicate = OBDADataFactoryImpl.getInstance().getTypePredicate(type);
			Predicate unifiedType = unifyTypes(ansTypes.get(j), typePredicate);
			ansTypes.set(j, unifiedType);
		} else if (term instanceof URIConstant) {
			ansTypes.set(j, OBDAVocabulary.XSD_STRING);
		} else if (term instanceof BNode) {
			ansTypes.set(j, OBDAVocabulary.XSD_STRING);
		} 
	}

	/**
	 * Unifies the input types
	 * 
	 * For instance,
	 * 
	 * [int, double] -> double
	 * [int, varchar] -> varchar
	 * [int, int] -> int
	 * 
	 * @param predicate
	 * @param typePred
	 * @return
	 */
	private Predicate unifyTypes(Predicate type1, Predicate type2) {

		if (type1 == null) {
			return type2;
		} else if (type1.equals(type2)) {
			return type1;
		} else if (dataTypePredicateUnifyTable.contains(type1, type2)) {
			return dataTypePredicateUnifyTable.get(type1, type2);
		} else if (type2 == null) {
			throw new NullPointerException("type2 cannot be null");
		} else {
			return OBDAVocabulary.XSD_STRING;
		}
	}
	
    
    
	@Override
	protected String getAlgebraConditionString(Function atom, QueryVariableIndex index) {
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
				return getNativeType(f.getFunctionSymbol());
			}
		} else if (term instanceof Variable){
			throw new RuntimeException("Cannot return the SQL type for: " + term);
		}
		
		// Return varchar for unknown
		return defaultSQLType;
	}

	//TODO: find a proper place for this method 
	private int getNativeType(Predicate p) {
		return predicateSQLTypes.containsKey(p) ? predicateSQLTypes.get(p).intValue() : defaultSQLType;
	}

	@Override
	protected String getNativeLexicalForm(URIConstant uc) {
		return jdbcutil.getSQLLexicalForm(uc.getURI());
	}

	@Override
	protected String getNativeLexicalForm(ValueConstant ct) {
		return jdbcutil.getSQLLexicalForm(ct);
	}

	@Override
	protected String getArithmeticConditionString(Function atom, QueryVariableIndex index) {
		String expressionFormat = getArithmeticOperatorString(atom.getFunctionSymbol());

		Term term1 = atom.getTerm(0);
		Term term2 = atom.getTerm(1);
		
		String bracketsTemplate = "(%s)";
		String leftOp = String.format(bracketsTemplate, getNativeString(term1, index));
		String rightOp = String.format(bracketsTemplate, getNativeString(term2, index));
		String result = String.format(expressionFormat, leftOp, rightOp);
		//if (useBrackets) {
			return String.format(bracketsTemplate, result);
		//} else {
		//	return result;
		//}
	}

	@Override
	protected String getAggregateConditionString(Function atom, QueryVariableIndex index) {
		Predicate predicate = atom.getFunctionSymbol();
		Term term1 = atom.getTerm(0);

		if (predicate.equals(OBDAVocabulary.SPARQL_COUNT) || 
				predicate.equals(OBDAVocabulary.SPARQL_AVG) || 
				predicate.equals(OBDAVocabulary.SPARQL_SUM)) {
			
			String columnName; 
			if (term1.toString().equals("*")) {
				columnName = "*";
			}
			else {
				columnName = getNativeString(atom.getTerm(0), index);
			}
			
			return predicate.getName().toUpperCase() + "(" + columnName + ")";
		} 

		throw new RuntimeException("Unexpected function in the query: " + atom);
	}

	static String removeAllQuotes(String string) {
		while (string.startsWith("\"") && string.endsWith("\"")) {
			string = string.substring(1, string.length() - 1);
		}
		return string;
	}

	private static String removeQuotes(String string) {
		if ( (string.startsWith("\"") && string.endsWith("\"")) ||
			(string.startsWith("\'") && string.endsWith("\'")) ||
			(string.startsWith("`") && string.endsWith("`")) ) {
			return string.substring(1, string.length() - 1);
		}
		return string;
	}

	private static String getStringConcatenation(SQLDialectAdapter adapter,
			String[] params, QueryInfo queryInfo) {
		String toReturn = adapter.strconcat(params);
		if (adapter instanceof DB2SQLDialectAdapter) {
			/*
			 * A work around to handle DB2 (>9.1) issue SQL0134N: Improper use
			 * of a string column, host variable, constant, or function name.
			 * http
			 * ://publib.boulder.ibm.com/infocenter/db2luw/v9r5/index.jsp?topic
			 * =%2Fcom.ibm.db2.luw.messages.sql.doc%2Fdoc%2Fmsql00134n.html
			 */
			if (queryInfo.isDistinct() || queryInfo.isOrderBy()) {
				return adapter.sqlCast(toReturn, Types.VARCHAR);
			}
		}
		return toReturn;
	}

	@Override
	protected String getLanguageConditionString(Function atom, QueryVariableIndex index) {
		Variable var = (Variable) atom.getTerm(0);
		String langC = getColumnName(var, index);
		
		String langColumn = langC.replaceAll("`$", "Lang`");
		return langColumn;
	}

	@Override
	protected String getCastConditionString(Function atom, QueryVariableIndex index) {
		String columnName = getNativeString(atom.getTerm(0), index);
		
		if (isStringColType(atom, (SQLQueryVariableIndex) index)) {
			return columnName;
		} 
		
		else {
			String datatype = ((Constant) atom.getTerm(1)).getValue();
			
			int sqlDatatype = -1;
			if (datatype.equals(OBDAVocabulary.XSD_STRING_URI)) {
				sqlDatatype = Types.VARCHAR;
			}
			return sqladapter.sqlCast(columnName, sqlDatatype);
		}
	}

	@Override
	protected String getSTRConditionString(Function atom,
			QueryVariableIndex index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String convertTemplateToString(Function atom,
			QueryVariableIndex index) {
		// TODO Auto-generated method stub
		return null;
	}


}
