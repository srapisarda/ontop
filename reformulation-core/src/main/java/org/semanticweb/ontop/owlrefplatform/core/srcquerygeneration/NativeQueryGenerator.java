package org.semanticweb.ontop.owlrefplatform.core.srcquerygeneration;

import java.io.Serializable;
import java.util.List;

import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.OBDAException;

public interface NativeQueryGenerator extends Serializable {

	/**
	 * Translates the given datalog program into a source query, which can later
	 * be evaluated by a evaluation engine.
	 * 
	 * @param query
	 *            the datalog program
	 * @return the souce query
	 * @throws Exception
	 */
	public String generateSourceQuery(DatalogProgram query, List<String> signature) throws OBDAException;

}
