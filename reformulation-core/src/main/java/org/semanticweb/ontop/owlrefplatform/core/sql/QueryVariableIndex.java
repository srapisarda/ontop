package org.semanticweb.ontop.owlrefplatform.core.sql;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.semanticweb.ontop.model.AlgebraOperatorPredicate;
import org.semanticweb.ontop.model.BooleanOperationPredicate;
import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.owlrefplatform.core.queryevaluation.SQLDialectAdapter;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.DataDefinition;
import org.semanticweb.ontop.sql.TableDefinition;
import org.semanticweb.ontop.sql.ViewDefinition;
import org.semanticweb.ontop.sql.api.Attribute;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

public class QueryVariableIndex {

	Map<Variable, String> variableColumnIndex = new HashMap<Variable, String>();

	public QueryVariableIndex(CQIE cq, DBMetadata metadata) {
		
		computeColumnIndex(cq, metadata);
	}
	
	protected void computeColumnIndex(CQIE cq, DBMetadata metadata) {
		List<Function> body = cq.getBody();
		for (Function atom : body) {
			computeColumnIndexFromAtom(atom, metadata);
		}
	}

	private void computeColumnIndexFromAtom(Function atom, DBMetadata metadata) {
		if (!atom.isDataFunction()) {
			return;
		}
	
		Predicate tablePredicate = atom.getFunctionSymbol();
		String tableName = tablePredicate.getName();
		DataDefinition def = metadata.getDefinition(tableName);

		if (def == null) {
			return;
		}
		
		if (atom.getTerms().size() != def.getAttributes().size()) {
			throw new RuntimeException("Mismatch between " + atom + " and database metadata " + metadata);
		}
		
		int i=0;
		for (Term term : atom.getTerms()) {
			if ( term instanceof Variable ) {
				Attribute attribute = def.getAttribute(i+1);
				variableColumnIndex.put((Variable)term, attribute.getName());
			}
			i++;
		}
	}

	public String getColumnName(Variable var) {
		return variableColumnIndex.containsKey(var) ? variableColumnIndex.get(var) : null;
	}

}
