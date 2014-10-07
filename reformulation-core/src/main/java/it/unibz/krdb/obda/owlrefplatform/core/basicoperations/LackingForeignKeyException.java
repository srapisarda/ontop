package it.unibz.krdb.obda.owlrefplatform.core.basicoperations;

/*
 * #%L
 * ontop-reformulation-core
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

import it.unibz.krdb.sql.Reference;

/**
 * Used for signalling lacking foreign keys in the metadata, which do not constitute a problem.
 * These foreign keys are not used in the mappings, so do not need to be reported
 * @author dagc
 *
 */
public class LackingForeignKeyException extends BrokenForeignKeyException {

	
	public LackingForeignKeyException(Reference reference, String message) {
		super(reference, message);
	}
	
}
