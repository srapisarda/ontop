package it.unibz.inf.ontop.exception;

/*
 * #%L
 * ontop-obdalib-core
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


/**
 * This exception is thrown when a mapping contains an error
 * (that is, when the query is not a valid SQL query for the data source)
 */
public  class MappingQueryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MappingQueryException(String message, Object object) {
        super(message + " "  + object.toString());
    }
}