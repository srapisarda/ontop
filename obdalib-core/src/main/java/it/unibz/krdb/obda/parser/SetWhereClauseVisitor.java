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


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;

/**
 * Visitor class to retrieve the WHERE clause of the SELECT statement
 *
 * BRINGS TABLE NAME / SCHEMA / ALIAS AND COLUMN NAMES in the WHERE clause into NORMAL FORM
 */
public class SetWhereClauseVisitor {


    public SetWhereClauseVisitor(Select selectQuery, final Expression whereClause) {

        selectQuery.getSelectBody().accept(new SelectVisitor() {
            @Override
            public void visit(PlainSelect plainSelect) {
                plainSelect.setWhere(whereClause);
            }
            @Override
            public void visit(SetOperationList setOpList) {
                // we do not consider the case of UNION
                // ROMAN (22 Sep 2015): not sure why it is applied to the first one only
                setOpList.getPlainSelects().get(0).accept(this);
            }
            @Override
            public void visit(WithItem withItem) {
                // we do not consider the case for WITH
            }
        });
    }
}
