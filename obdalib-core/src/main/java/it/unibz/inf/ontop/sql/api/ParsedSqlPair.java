package it.unibz.inf.ontop.sql.api;

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

import java.util.Objects;

/**
 * Created by Salvatore Rapisarda
 * This class is a copy of com.sun.tools.javac.util.Pair
 * and has been created to avoid compiler problems
 */

public class ParsedSqlPair<A, B> {
    public A getFst() {
        return fst;
    }

    public B getSnd() {
        return snd;
    }

    final A fst;
    final B snd;

    public ParsedSqlPair(A var1, B var2) {
        this.fst = var1;
        this.snd = var2;
    }

    public String toString() {
        return "ParsedSqlPair[" + this.fst + "," + this.snd + "]";
    }

    public boolean equals(Object var1) {
        return var1 instanceof ParsedSqlPair && Objects.equals(this.fst, ((ParsedSqlPair)var1).fst) && Objects.equals(this.snd, ((ParsedSqlPair)var1).snd);
    }

    public int hashCode() {
        return this.fst == null?(this.snd == null?0:this.snd.hashCode() + 1):(this.snd == null?this.fst.hashCode() + 2:this.fst.hashCode() * 17 + this.snd.hashCode());
    }

    public static <A, B> ParsedSqlPair<A, B> of(A var0, B var1) {
        return new ParsedSqlPair<>(var0, var1);
    }
}