package org.semanticweb.ontop.api.io;

import java.io.IOException;

import org.semanticweb.ontop.exception.InvalidMappingExceptionWithIndicator;
import org.semanticweb.ontop.exception.InvalidPredicateDeclarationException;
import org.semanticweb.ontop.io.ModelIOManager;
import org.semanticweb.ontop.model.OBDADataFactory;
import org.semanticweb.ontop.model.SQLOBDAModel;
import org.semanticweb.ontop.model.impl.OBDADataFactoryImpl;

import org.junit.Test;

public class ModelIOManagerTest {

	@Test
	public void testSpaceBeforeEndCollectionSymbol() throws IOException,
			InvalidPredicateDeclarationException, InvalidMappingExceptionWithIndicator {
		OBDADataFactory fac = OBDADataFactoryImpl.getInstance();
		SQLOBDAModel obdaModel = fac.getOBDAModel();
		ModelIOManager ioManager = new ModelIOManager(obdaModel);
		ioManager
				.load("src/test/resources/format/obda/unusualCollectionEnding.obda");
	}

	@Test(expected = IOException.class)
	public void testEndCollectionSymbolRequirement() throws IOException,
			InvalidPredicateDeclarationException, InvalidMappingExceptionWithIndicator {
		OBDADataFactory fac = OBDADataFactoryImpl.getInstance();
		SQLOBDAModel obdaModel = fac.getOBDAModel();
		ModelIOManager ioManager = new ModelIOManager(obdaModel);
		ioManager
				.load("src/test/resources/format/obda/missingCollectionEnding.obda");
	}

}