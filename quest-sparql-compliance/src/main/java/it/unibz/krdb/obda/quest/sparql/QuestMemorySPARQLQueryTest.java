package it.unibz.krdb.obda.quest.sparql;

/*
 * #%L
 * ontop-sparql-compliance
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import it.unibz.krdb.obda.owlrefplatform.core.QuestPreferences;
import junit.framework.Test;

import org.openrdf.query.Dataset;
import org.openrdf.repository.Repository;

import sesameWrapper.SesameVirtualRepo;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

public class QuestMemorySPARQLQueryTest extends SPARQLQueryParent {

	public static Test suite() throws Exception {
		return QuestManifestTestUtils.suite(new Factory() {
			public QuestMemorySPARQLQueryTest createSPARQLQueryTest(
					String testURI, String name, String queryFileURL,
					String resultFileURL, String mappingFileURL,
                    String dbFileURL, boolean laxCardinality) {
				return createSPARQLQueryTest(testURI, name, queryFileURL,
						resultFileURL, mappingFileURL, dbFileURL, laxCardinality, false);
			}

			public QuestMemorySPARQLQueryTest createSPARQLQueryTest(
					String testURI, String name, String queryFileURL,
					String resultFileURL, String mappingFileURL,
                    String dbFileURL, boolean laxCardinality, boolean checkOrder) {
				return new QuestMemorySPARQLQueryTest(testURI, name,
						queryFileURL, resultFileURL, mappingFileURL, dbFileURL, laxCardinality,
						checkOrder);
			}
		});
	}

	protected QuestMemorySPARQLQueryTest(String testURI, String name,
			String queryFileURL, String resultFileURL, String mappingFileURL,
            String dbFileURL, boolean laxCardinality, boolean checkOrder) {
		super(testURI, name, queryFileURL, resultFileURL, mappingFileURL,
                dbFileURL, laxCardinality, checkOrder);
	}

	@Override
	protected Repository newRepository() throws Exception {

        sqlConnection = DriverManager.getConnection("jdbc:h2:mem:triplestore", "sa", "");
        java.sql.Statement s = sqlConnection.createStatement();

        try {
            String text = new Scanner(new File(dbFileURL)).useDelimiter("\\A").next();
            s.execute(text);

        } catch (SQLException sqle) {
            System.out.println("Exception in creating db from script");
        }
        s.close();
        sqlConnection.close();

        SesameVirtualRepo repo = new SesameVirtualRepo("QuestSPARQLTest", mappingFileURL,
                new QuestPreferences());
        repo.initialize();
        return repo;
    }
}
