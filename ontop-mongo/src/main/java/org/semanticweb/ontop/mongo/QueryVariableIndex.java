package org.semanticweb.ontop.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.DataDefinition;
import org.semanticweb.ontop.sql.api.Attribute;

import com.google.common.collect.ImmutableMap;

public class QueryVariableIndex {

	final Map<Variable, String> variableColumnIndex;
	QueryVariableIndex(CQIE cq, DBMetadata metadata) {
	
		Map<Variable, String> index = computeColumnIndex(cq, metadata);
		variableColumnIndex = ImmutableMap.copyOf(index);
	}
	
	protected Map<Variable, String> computeColumnIndex(CQIE cq, DBMetadata metadata) {
		Map<Variable, String> index = new HashMap<>();
		List<Function> body = cq.getBody();
		for (Function atom : body) {
			index.putAll( computeColumnIndexFromAtom(atom, metadata) );
		}
		return index;
	}

	private Map<Variable, String> computeColumnIndexFromAtom(Function atom, DBMetadata metadata) {
		Map<Variable, String> varColumnIndex = new HashMap<>();
		if (!atom.isDataFunction()) {
			return varColumnIndex;
		}
	
		Predicate tablePredicate = atom.getFunctionSymbol();
		String tableName = tablePredicate.getName();
		DataDefinition def = metadata.getDefinition(tableName);

		if (atom.getTerms().size() != def.getAttributes().size()) {
			throw new RuntimeException("Mismatch between " + atom + " and database metadata " + metadata);
		}
		
		int i=0;
		for (Term term : atom.getTerms()) {
			if ( term instanceof Variable ) {
				Attribute attribute = def.getAttribute(i);
				varColumnIndex.put((Variable)term, attribute.getName());
			}
			i++;
		}
		return varColumnIndex;
	}

	public String getColumnName(Variable var) {
		return variableColumnIndex.containsKey(var) ? variableColumnIndex.get(var) : null;
	}
}
