package org.elasticsearch.es4sql.parse;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;


public class SQLParensIdentifierExpr extends SQLIdentifierExpr {

    public SQLParensIdentifierExpr() {
    }

    public SQLParensIdentifierExpr(String name) {
        super(name);
    }

    public SQLParensIdentifierExpr(SQLIdentifierExpr expr) {
        super(expr.getName());
    }
}
