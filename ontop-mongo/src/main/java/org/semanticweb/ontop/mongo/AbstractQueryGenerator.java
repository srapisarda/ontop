package org.semanticweb.ontop.mongo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URIConstant;
import org.semanticweb.ontop.model.ValueConstant;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.sql.DBMetadata;

public abstract class AbstractQueryGenerator {


	protected final DBMetadata metadata;

	public AbstractQueryGenerator(DBMetadata metadata) {
		this.metadata = metadata.clone();
	}
	
	/**
	 * Returns the native template string for the boolean operator, including placeholders
	 * for the terms to be used, e.g., %s = %s, %s IS NULL, etc.
	 * 
	 * @param functionSymbol
	 * @return
	 */
	abstract public String getBooleanOperatorTemplate(Predicate functionSymbol);

	

	/**
	 * Returns the string representation of conditions. The most interesting ones are Boolean conditions.
	 * @param atoms
	 * @param index
	 * @return
	 */
	 
	protected Set<String> getConditionsString(CQIE cq) {
		QueryVariableIndex index = new QueryVariableIndex(cq, this.metadata);

		Set<String> conditions = new HashSet<String>();
		for (Function atom : cq.getBody()) {
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
		else {
			// a data predicate
			return null;
		}
	}

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
