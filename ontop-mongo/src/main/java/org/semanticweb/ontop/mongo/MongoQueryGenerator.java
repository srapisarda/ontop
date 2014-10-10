package org.semanticweb.ontop.mongo;

import java.util.List;

import org.semanticweb.ontop.model.DatalogProgram;
import org.semanticweb.ontop.model.OBDAException;
import org.semanticweb.ontop.owlrefplatform.core.srcquerygeneration.NativeQueryGenerator;
import org.semanticweb.ontop.sql.DBMetadata;
import org.semanticweb.ontop.sql.TableDefinition;

public class MongoQueryGenerator implements NativeQueryGenerator {

	private DBMetadata dbMetadata;
	
	public MongoQueryGenerator(DBMetadata metadata) {
		dbMetadata = metadata;
	}
	
	@Override
	public String generateSourceQuery(DatalogProgram query,
			List<String> signature) throws OBDAException {

		
		String mongo = "find(%s)";
		String selectPart = "";
		mongo = String.format (mongo, selectPart);
		return mongo;
	}

}
