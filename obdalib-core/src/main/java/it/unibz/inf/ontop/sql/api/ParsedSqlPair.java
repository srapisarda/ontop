package it.unibz.inf.ontop.sql.api;

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