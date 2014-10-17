package org.semanticweb.ontop.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.BSONTimestamp;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URIConstant;
import org.semanticweb.ontop.model.ValueConstant;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.Predicate.COL_TYPE;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.owlrefplatform.core.srcquerygeneration.NativeQueryGenerator;
import org.semanticweb.ontop.sql.DBMetadata;

import com.google.common.collect.ImmutableMap;


public class MongoQueryGenerator extends AbstractQueryGenerator implements NativeQueryGenerator {

	private static String DATA_TYPE_OPERATOR = "\"%s\" : { $type: %s }";

	static final Map<Predicate, String> booleanPredicateToQueryString;
	static {
		Map<Predicate, String> temp = new HashMap<>();
		
		temp.put(OBDAVocabulary.EQ, "\"%s\" : %s");
		temp.put(OBDAVocabulary.NEQ, "\"%s\" : {$ne : %s}");
		temp.put(OBDAVocabulary.GT, "\"%s\" : {$gt : %s}");
		temp.put(OBDAVocabulary.GTE, "\"%s\" : {$gte : %s}");
		temp.put(OBDAVocabulary.LT, "\"%s\" : {$lt : %s}");
		temp.put(OBDAVocabulary.LTE, "\"%s\" : {$lte : %s}");
		temp.put(OBDAVocabulary.AND, "%s, %s");
		temp.put(OBDAVocabulary.OR, "$or : [ {%s}, {%s} ]");
		temp.put(OBDAVocabulary.NOT, "\"%s\" : {$not : %s}");
		
		//see: http://docs.mongodb.org/manual/faq/developers/#faq-developers-query-for-nulls
		temp.put(OBDAVocabulary.IS_NULL, "\"%s\" : {$type : 10}");
		temp.put(OBDAVocabulary.IS_NOT_NULL, "\"%s\" : {$not : {$type : 10} }");
		
		//TODO: modify the templates below
		temp.put(OBDAVocabulary.IS_TRUE, "%s IS TRUE");
		temp.put(OBDAVocabulary.SPARQL_LIKE, "%s LIKE %s");
		//TODO: what should we do here?
		//we do not need the operator for regex, it should not be used, because the sql adapter will take care of this
		temp.put(OBDAVocabulary.SPARQL_REGEX, ""); 
				
		booleanPredicateToQueryString = ImmutableMap.copyOf(temp);
	}
	
	static final Map<Predicate, String> arithmeticPredicateToQueryString;
	static {
		Map<Predicate, String> temp = new HashMap<>();
		
		temp.put(OBDAVocabulary.ADD, "$add: [%s, %s]");
		temp.put(OBDAVocabulary.SUBSTRACT, "$substract: [%s, %s]");
		temp.put(OBDAVocabulary.MULTIPLY, "$multiply: [%s, %s]");
				
		arithmeticPredicateToQueryString = ImmutableMap.copyOf(temp);
	}

	
	
	
	
	public MongoQueryGenerator(DBMetadata metadata) {
		super(metadata);
	}
	
	@Override
	public String generateSourceQuery(DatalogProgram query, List<String> signature) throws OBDAException {

		StringBuilder conditions = new StringBuilder();
		for (CQIE cq : query.getRules()) {
			Set<String> booleanTypeConditions = getConditionsString(cq);
			for(String condition : booleanTypeConditions ) {
				conditions.append(condition).append(", ");
			}
		}
		
		String mongo = "{%s}";
		String selectPart = conditions.toString();		
		mongo = String.format (mongo, selectPart);
		return mongo;
	}
	
	
	/***
	 * Returns the Mongo criteria for an atom representing a Boolean condition.
	 */
	@Override
	protected String getBooleanConditionString(Function atom, QueryVariableIndex index) {
		if (!atom.isBooleanFunction()) {
			throw new RuntimeException("Invoked for non-Boolean function " + atom.getFunctionSymbol() + "!");
		}

		if (atom.getArity() == 1) {
			return getUnaryBooleanConditionString(atom, index);
		} else if (atom.getArity() == 2) {
			return getBinaryBooleanConditionString(atom, index);
		} else {
			if (atom.getFunctionSymbol() == OBDAVocabulary.SPARQL_REGEX) {
				return getRegularExpressionString(atom, index);
			}

			throw new RuntimeException("The Boolean function " + atom.getFunctionSymbol() + " is not supported yet!");
		}
	}

	private String getBinaryBooleanConditionString(Function atom, QueryVariableIndex index) {
		Predicate booleanPredicate = atom.getFunctionSymbol();
		String expressionFormat = getBooleanOperatorTemplate(booleanPredicate);
		
		Term left = atom.getTerm(0);
		Term right = atom.getTerm(1);
		String leftOp = "";
		String rightOp = "";

		// AND, OR
		if (booleanPredicate.equals(OBDAVocabulary.AND) || booleanPredicate.equals(OBDAVocabulary.OR)) {
			if (!(left instanceof Function && right instanceof Function)) {
				throw new RuntimeException("Boolean expression " + atom + " should have functions as operands!");
			}
			leftOp = getBooleanConditionString((Function) left, index);
			rightOp = getBooleanConditionString((Function) right, index);
		}
		
		// EQ, NEQ, GT, GTE, LT, LTE
		else {
			if (! (right instanceof ValueConstant || right instanceof URIConstant) || !( left instanceof Variable )) {
				//TODO check it does not happen when possible, i.e, eliminate self-joins when possible
				throw new RuntimeException("Boolean expression " + atom + " cannot be translated to a mongo query!");
			}
			leftOp = getColumnName((Variable)left, index);
			rightOp = getNativeString(right, index);
		}
		
		return String.format(expressionFormat, leftOp, rightOp);
	}

	private String getUnaryBooleanConditionString(Function atom, QueryVariableIndex index) {

		Predicate booleanPredicate = atom.getFunctionSymbol();
		String expressionFormat = getBooleanOperatorTemplate(booleanPredicate);
		
		Term term = atom.getTerm(0);
		String operand = "";
		
		// NOT
		if (booleanPredicate.equals(OBDAVocabulary.NOT)) {
			if (!(term instanceof Function)) {
				//
			}
			operand = getBooleanConditionString((Function) term, index);
		}
		
		// IS NULL, IS NOT NULL
		else {
			operand = getNativeString(term, index);
		}
		
		return String.format(expressionFormat, operand);
	}

	private String getRegularExpressionString(Function atom, QueryVariableIndex index) {
		// TODO
		return null;
	}

	@Override
	protected String getDataTypeConditionString(Function atom, QueryVariableIndex index) {
		if (! atom.isDataTypeFunction() ) {
			throw new RuntimeException("Invoked for non-datatype function " + atom.getFunctionSymbol() + "!");
		}
		
		Predicate dataTypePredicate = atom.getFunctionSymbol();
		//check here
		Term term = atom.getTerm(0);
		if (! (term instanceof Variable) ) {
			throw new RuntimeException("Data typing " + atom + " cannot be translated to a mongo query!");
		}
		String column = getColumnName((Variable)term, index);
		
		//TODO: change here (when an appropriate method exists in a proper class
		int type = (new MongoSchemaExtractor()).getNativeType(dataTypePredicate);
		return String.format(DATA_TYPE_OPERATOR, column, type);
		
	}
	
	@Override
	protected String getArithmeticConditionString(Function atom, QueryVariableIndex index) {
		if (! atom.isArithmeticFunction() ) {
			throw new RuntimeException("Invoked for non-arithmetic function " + atom.getFunctionSymbol() + "!");
		}
		
		Predicate arithmeticPredicate = atom.getFunctionSymbol();		
		throw new RuntimeException("The Boolean function " + arithmeticPredicate + " is not supported yet!");
		
		// For numerical operators, e.g., MUTLIPLY, SUBSTRACT, ADDITION
	//	String expressionFormat = getNumericalOperatorString(arithmeticPredicate);
	//	Term left = atom.getTerm(0);
	//	Term right = atom.getTerm(1);
	//	String leftOp = getConditionString(left, index);
	//	String rightOp = getConditionString(right, index);
	//	return String.format(expressionFormat, leftOp, rightOp);
	}


	@Override
	protected String getAlgebraConditionString(Function atom, QueryVariableIndex index) {
		// TODO Auto-generated method stub
		return null;
	}

	private static String toMongoString(String subString) {
		return String.format("\"%s\"", subString);
	}

	protected String getNativeLexicalForm(URIConstant uc){
		return toMongoString(uc.toString());
	}

	protected String getNativeLexicalForm(ValueConstant constant) {
		String mongoRepresentation = null;
		if (constant.getType() == COL_TYPE.BNODE 
				|| constant.getType() == COL_TYPE.LITERAL 
				|| constant.getType() == COL_TYPE.OBJECT
				|| constant.getType() == COL_TYPE.STRING) {
			//TODO: check here
			mongoRepresentation = toMongoString(constant.getValue());
		} else if (constant.getType() == COL_TYPE.BOOLEAN) {
			mongoRepresentation = getMongoLexicalFormBoolean(constant);
		} else if (constant.getType() == COL_TYPE.DECIMAL 
				|| constant.getType() == COL_TYPE.DOUBLE
				|| constant.getType() == COL_TYPE.INTEGER) {
			mongoRepresentation = constant.getValue();
		} else if (constant.getType() == COL_TYPE.DATETIME) {
			//TODO: check here
			mongoRepresentation = toMongoString(constant.getValue());
		} else {
			mongoRepresentation = toMongoString(constant.getValue());
		}
		return mongoRepresentation;
	}

	public String getMongoLexicalFormBoolean(ValueConstant rdfliteral) {
		String value = rdfliteral.getValue().toLowerCase();
		String lexicalForm = null;
		if (value.equals("1") || value.equals("true") || value.equals("t")) {
				lexicalForm = "true";
			
		} else if (value.equals("0") || value.equals("false") || value.equals("f")) {
				lexicalForm = "false";
		} else {
			throw new RuntimeException("Invalid lexical form for xsd:boolean. Found: " + value);
		}
		return lexicalForm;
	}

	/**
	 * Returns the Mongo template string for the boolean operator, including placeholders
	 * for the terms to be used, e.g., %s = %s, %s IS NULL, etc.
	 * 
	 * @param booleanPredicate
	 * @return
	 */
	@Override
	public String getBooleanOperatorTemplate(Predicate booleanPredicate) {
		if (booleanPredicateToQueryString.containsKey(booleanPredicate)) {
			return booleanPredicateToQueryString.get(booleanPredicate);
		}

		throw new RuntimeException("Unknown boolean operator: " + booleanPredicate);
	}

	
	
	
	
	
}
	