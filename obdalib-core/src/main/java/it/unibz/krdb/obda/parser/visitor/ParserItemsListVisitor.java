package it.unibz.krdb.obda.parser.visitor;
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


import it.unibz.krdb.obda.parser.exception.ParseException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Created by salvo on 13/01/2016.
 */
public class ParserItemsListVisitor implements net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor {

    private final TableExpressionVisitor tableExpressionVisitor;

    public ParserItemsListVisitor(TableExpressionVisitor tableExpressionVisitor) {
        this.tableExpressionVisitor = tableExpressionVisitor;
    }

    @Override
    public void visit(ExpressionList expressionList) {
        for (Expression expression : expressionList.getExpressions())
            expression.accept(tableExpressionVisitor);
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        throw new ParseException(multiExprList);
        //for (ExpressionList exprList : multiExprList.getExprList())
        //    exprList.accept(this);
    }

    @Override
    public void visit(SubSelect subSelect) {

        if (!(subSelect.getSelectBody() instanceof PlainSelect))
            throw new ParseException(subSelect);

        PlainSelect subSelBody = (PlainSelect)subSelect.getSelectBody();

        // only very simple subqueries are supported at the moment
        if (subSelBody.getJoins() != null || subSelBody.getWhere() != null)
            throw new ParseException(subSelect);

        // TODO: Review the code above
        //  subSelBody.accept(selectVisitor); TODO: this has been commented out because selectVisitor dependency unresolved


    }
}
