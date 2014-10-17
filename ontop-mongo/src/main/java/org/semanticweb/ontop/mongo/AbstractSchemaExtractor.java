package org.semanticweb.ontop.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.semanticweb.ontop.model.CQIE;
import org.semanticweb.ontop.model.DataTypePredicate;
import org.semanticweb.ontop.model.Function;
import org.semanticweb.ontop.model.Predicate;
import org.semanticweb.ontop.model.Term;
import org.semanticweb.ontop.model.URITemplatePredicate;
import org.semanticweb.ontop.model.Variable;
import org.semanticweb.ontop.sql.api.Attribute;

public abstract class AbstractSchemaExtractor {

	/*
	 * TODO:
	
	 1) we need an attribute ontology?
	 2) we need a method (from a different class?)
	 	int type = getDomainTypeFromOntology(predicate, ontology)

	 3) what structure for the attributes in analyzeTargetQuery and other methods
	 
	 4) putAll - keep it? (may be rename)
	 
	 5) there is one "problem" with the types, BSON and SQL types are different, but Attribute understands only SQL types I guess
	 */
	
	
	protected Map<String, Attribute> analyzeTargetQuery(CQIE targetQuery) {
		Map<String, Attribute> queryVariables = new HashMap<>();
		
		List<Function> bodyAtoms = targetQuery.getBody();
		for (Function atom : bodyAtoms) {
			putAll( queryVariables, analyzeBodyAtom(atom) );
		}

		return queryVariables;
	}

	private Map<String, Attribute> analyzeBodyAtom(Function atom) {
		Map<String, Attribute> atomVariables = new HashMap<>();

		List<Term> terms = atom.getTerms();
		
		// We assume that terms with position 0 correspond to subject. 
		// We use this position to understand whether an attribute corresponding to
		// a path of values in a mongo document, e.g., contact.email, name, code, etc, 
		// can be "considered" to be a primary key
		// 
		int termPosition = 0;
		for (Term term: terms) {
			putAll( atomVariables, analyzeTerm(term, termPosition) );
			termPosition++;
		}
		
		return atomVariables;
	}

	/**
	 * 
	 * @param term
	 * @param termPosition
	 * 			the position of the term in the enclosing atom. If it is 0, then this term is a subject 
	 * 			in a triple, and it is likely that the Attribute corresponding to this term is a primary key 
	 * @return
	 */
	private Map<String, Attribute> analyzeTerm(Term term, int termPosition) {
		Map<String, Attribute> termVariables = new HashMap<>();

		if (term instanceof Function) {
			Function function = (Function) term;

			Predicate functionSymbol = function.getFunctionSymbol();
			if (functionSymbol instanceof URITemplatePredicate) {
				//Term uriTemplateTerm = function.getTerm(0);
				Variable variable = (Variable)function.getTerm(1);
				boolean primaryKey = termPosition == 0 ? true : false;
				termVariables.put( variable.getName(), new Attribute(variable.getName(), getDefaultNativeType(), primaryKey, null) );
			} 
			else if (functionSymbol instanceof DataTypePredicate) {
				Variable variable = (Variable)function.getTerm(0);
				int nativeType = getNativeType(functionSymbol);
				termVariables.put( variable.getName(), new Attribute(variable.getName(), nativeType) );
			}
			else {
				//TODO : what to do here?
				// throw an Exception
			}
		}
		// term is a neither a URI, nor has a specified data type. Check if it is a Variable
		else if (term instanceof Variable) {
			Variable variable = (Variable) term;
			/*
			 * copied from #MappingDataTypeRepair.insertDataTyping
			 * 
			
            Predicate dataTypeFunctor = null;

            //check in the ontology if we have already information about the datatype

            Function normal = equivalenceMap.getNormal(atom);
                //Check if a datatype was already assigned in the ontology
            dataTypeFunctor= dataTypesMap.get(normal.getFunctionSymbol());



            // If the term has no data-type predicate then by default the
            // predicate is created following the database metadata of
            // column type.
            if(dataTypeFunctor==null || isBooleanDB2(dataTypeFunctor) ){

                dataTypeFunctor = getDataTypeFunctor(variable);
            }

			Term newTerm = dfac.getFunction( dataTypeFunctor, variable);
			atom.setTerm(1, newTerm);
			*/

			// we need something like this
			// int type = getTypeFromOntology(predicate, ontology)
			boolean primaryKey = termPosition == 0 ? true : false;
			termVariables.put( variable.getName(), new Attribute(variable.getName(), getDefaultNativeType(), primaryKey, null) );
			
		}
		else {
			//error
		}
		
		return termVariables;
	}


	// TODO: check this
	protected void putAll(Map<String, Attribute> allVariables, Map<String, Attribute> newVariables) {
		for (Entry<String, Attribute> pair : newVariables.entrySet()) {
			
			if ( allVariables.containsKey(pair.getKey()) && allVariables.get(pair.getKey()).getType() != pair.getValue().getType() ) {
				Attribute oldAttr = allVariables.get(pair.getKey());
				Attribute newAttr = pair.getValue();

				// the type is specialized in newVariables, so we update it in allVariables
				if (oldAttr.getType() == getDefaultNativeType()) {
					// TODO: we prefer to keep primary key flag, or not?
					allVariables.put(pair.getKey(), 
							new Attribute(oldAttr.getName(), newAttr.getType(), oldAttr.isPrimaryKey() || newAttr.isPrimaryKey(), null));
				}
				// the type is already specialized in allVariables
				else if (newAttr.getType() == getDefaultNativeType()) {
					// TODO: do we keep primary key flag?
					// otherwise do nothing
					allVariables.put(pair.getKey(), 
							new Attribute(oldAttr.getName(), oldAttr.getType(), oldAttr.isPrimaryKey() || newAttr.isPrimaryKey(), null));
				}
				else {
					// TODO: type conflict		
					//throw new Exception();
				}
			}
			else {
				allVariables.put(pair.getKey(), pair.getValue());
			}
		}
		
		// alternatively:
		// allVariables.putAll(newVariables);
	}


	protected abstract int getNativeType(Predicate functionSymbol);
	protected abstract int getDefaultNativeType();
}
