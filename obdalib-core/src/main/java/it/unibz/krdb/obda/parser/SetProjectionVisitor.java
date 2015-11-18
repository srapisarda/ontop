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


import it.unibz.krdb.sql.api.ProjectionJSQL;
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
public class SetProjectionVisitor{
    private boolean unsupported = false;
    final ProjectionJSQL projection;

    /**
     *
     * @param select
     * @param projection
     */
    public SetProjectionVisitor(Select select, final ProjectionJSQL projection) {
        this.projection = projection;
        select.getSelectBody().accept(selectVisitor);
    }


    public boolean isSupported(){
        return  !unsupported;
    }

    private void unsupported(Object o) {
        System.out.println(this.getClass() + " DOES NOT SUPPORT " + o);
        unsupported = true;
    }

    private SelectVisitor selectVisitor = new SelectVisitor() {

        @Override
        public void visit(PlainSelect plainSelect) {
            if (projection.getType().equals("select distinct on")) {
                List<SelectItem> distinctList = new ArrayList<>();

                for (SelectExpressionItem seItem : projection.getColumnList())
                    distinctList.add(seItem);

                Distinct distinct = new Distinct();
                distinct.setOnSelectItems(distinctList);
                plainSelect.setDistinct(distinct);
            } else if (projection.getType().equals("select distinct")) {
                Distinct distinct = new Distinct();
                plainSelect.setDistinct(distinct);

                plainSelect.getSelectItems().clear();
                plainSelect.getSelectItems().addAll(projection.getColumnList());
            } else {
                plainSelect.getSelectItems().clear();
                List<SelectExpressionItem> columnList = projection.getColumnList();
                if (!columnList.isEmpty()) {
                    plainSelect.getSelectItems().addAll(columnList);
                } else {
                    plainSelect.getSelectItems().add(new AllColumns());
                }
            }
        }

        @Override
        public void visit(SetOperationList setOpList) {
            unsupported = true;
            setOpList.getPlainSelects().get(0).accept(this);
        }

        @Override
        public void visit(WithItem withItem) {
            withItem.getSelectBody().accept(this);
        }
    };
}
