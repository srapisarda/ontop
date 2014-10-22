package org.semanticweb.ontop.owlrefplatform.core.sql;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.NumericalOperationPredicate;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URIConstant;
import org.semanticweb.ontop.model.ValueConstant;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.model.impl.OBDAVocabulary;
import org.semanticweb.ontop.sql.DBMetadata;

public abstract class AbstractQueryGenerator {


	protected final DBMetadata metadata;

	public AbstractQueryGenerator(DBMetadata metadata) {
		this.metadata = metadata;
		//if we clone, it does not work. Seems that metadata is being changed after an instance of Query generator is created 
		//see e.g. #OracleRegexpTestSpace.testSparql2OracleRegexWhere()
	}
	
	/**
	 * Returns the native template string for the boolean operator, including placeholders
	 * for the terms to be used, e.g., %s = %s, %s IS NULL, etc.
	 * 
	 * @param functionSymbol
	 * @return
	 */
	abstract public String getBooleanOperatorTemplate(Predicate functionSymbol);

	
	abstract public String getArithmeticOperatorString(Predicate arithmeticPredicate);
		
		
	/**
	 * Returns the string representation of conditions. The most interesting ones are Boolean conditions.
	 * @param atoms
	 * @param index
	 * @return
	 */
	 
	protected Set<String> getConditionsString(List<Function> atoms, QueryVariableIndex index) {
		Set<String> conditions = new HashSet<String>();
		for (Function atom : atoms) {
			String condition = getConditionString(atom, index);
			if ( condition != null ) {
				conditions.add(condition);
			}
		}
		return conditions;
	}


	protected String getConditionString(Function atom, QueryVariableIndex index) {
		
		if (atom.isBooleanFunction()) {
			return getBooleanConditionString(atom, index);
		} 
		else if (atom.isDataTypeFunction()) {
			return getDataTypeConditionString(atom, index);
		}
		else if (atom.isArithmeticFunction()) {
			return getArithmeticConditionString(atom, index);
		}
		else if (atom.isAlgebraFunction()) {
			return getAlgebraConditionString(atom, index);
		}
		else if (atom.getFunctionSymbol().isAggregationPredicate()) {
			return getAggregateConditionString(atom, index);
		}
		else if (atom.getFunctionSymbol().equals(OBDAVocabulary.SPARQL_LANG)) {
			return getLanguageConditionString(atom, index);
		}
		else if (atom.getFunctionSymbol().equals(OBDAVocabulary.QUEST_CAST)) {
			return getCastConditionString(atom, index);
		}
		else if (atom.getFunctionSymbol().equals(OBDAVocabulary.SPARQL_STR)) {
			return getSTRConditionString(atom, index);
		}
		//TODO are they supposed to be in the body of the program?
		else if (atom.getFunctionSymbol().getName().equals(OBDAVocabulary.QUEST_URI) ||
				atom.getFunctionSymbol().getName().equals(OBDAVocabulary.QUEST_BNODE)) {
			return convertTemplateToString(atom, index);
		}
		else {
			// a data predicate
			return null;
		}
	}

	protected abstract String getCastConditionString(Function atom, QueryVariableIndex index);

	protected abstract String getSTRConditionString(Function atom, QueryVariableIndex index);

	protected abstract String convertTemplateToString(Function atom, QueryVariableIndex index);

	protected abstract String getLanguageConditionString(Function atom, QueryVariableIndex index);

	protected abstract String getAggregateConditionString(Function atom, QueryVariableIndex index);

	protected abstract String getAlgebraConditionString(Function atom, QueryVariableIndex index) ;

	protected abstract String getArithmeticConditionString(Function atom, QueryVariableIndex index);

	protected abstract String getDataTypeConditionString(Function atom, QueryVariableIndex index);

	protected abstract String getBooleanConditionString(Function atom, QueryVariableIndex index);

	
	protected String getNativeString(Term term, QueryVariableIndex index) {
		if (term instanceof Function) {
			return getConditionString((Function)term, index);
		}
		else if (term instanceof Variable) {
			return getColumnName((Variable)term, index);
		}
		else if (term instanceof ValueConstant) {
			return getNativeLexicalForm((ValueConstant) term);
		} 
		else if (term instanceof URIConstant) {
			return getNativeLexicalForm((URIConstant) term);
		}

		return null;
	}
		
	protected String getColumnName(Variable var, QueryVariableIndex index) {
		String column = index.getColumnName(var);		
		if (column == null) {
			throw new RuntimeException("Unbound variable found in WHERE clause: " + var);
		}
		return column;
	}

	
	protected abstract String getNativeLexicalForm(URIConstant uc);

	protected abstract String getNativeLexicalForm(ValueConstant ct);



}
