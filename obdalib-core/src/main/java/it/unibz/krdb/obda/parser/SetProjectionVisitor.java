package it.unibz.krdb.obda.parser;
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


import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor to retrieve the projection of the given select statement. (SELECT... FROM).<br>
 *
 * BRINGS TABLE NAME / SCHEMA / ALIAS AND COLUMN NAMES in the FROM clause into NORMAL FORM
 *
 * Since the current release does not support Function, we throw a ParserException, when a function is present
 *
 */
public class SetProjectionVisitor {

	/**
     *
     * @param select
     * @param projection
     */
    public SetProjectionVisitor(Select select, final List<SelectItem> columnList) {
        select.getSelectBody().accept(new SelectVisitor() {

        @Override
        public void visit(PlainSelect plainSelect) {
            plainSelect.getSelectItems().clear();
            if (!columnList.isEmpty()) {
                plainSelect.getSelectItems().addAll(columnList);
            } else {
                plainSelect.getSelectItems().add(new AllColumns());
            }
        }

        @Override
        public void visit(SetOperationList setOpList) {
            setOpList.getPlainSelects().get(0).accept(this);
        }

        @Override
        public void visit(WithItem withItem) {
            withItem.getSelectBody().accept(this);
        }
    });
    }
}
