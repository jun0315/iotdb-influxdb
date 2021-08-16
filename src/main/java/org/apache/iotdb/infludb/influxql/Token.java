package org.apache.iotdb.infludb.influxql;

public enum Token {

    // ILLEGAL Token, EOF, WS are Special InfluxQL tokens.
    ILLEGAL,
    EOF,
    WS,
    COMMENT,

    literalBeg,
    // IDENT and the following are InfluxQL literal tokens.
    IDENT,// main
    BOUNDPARAM,// $param
    NUMBER,// 12345.67
    INTEGER,// 12345
    DURATIONVAL,// 13h
    STRING,// "abc"
    BADSTRING,// "abc
    BADESCAPE,// \q
    TRUE,// true
    FALSE,// false
    REGEX,// Regular expressions
    BADREGEX,// `.*
    literalEnd,
    operatorBeg,
    // ADD and the following are InfluxQL Operators
    ADD,// +
    SUB,// -
    MUL,// *
    DIV,// /
    MOD,// %
    BITWISE_AND,// &
    BITWISE_OR,// |
    BITWISE_XOR,// ^

    AND("and"),// AND
    OR("or"),// OR

    EQ("="),// =
    NEQ,// !=
    EQREGEX,// =~
    NEQREGEX,// !~
    LT,// <
    LTE,// <=
    GT,// >
    GTE,// >=
    operatorEnd,
    LPAREN,// (
    RPAREN,// )
    COMMA,// ,
    COLON,// :
    DOUBLECOLON,// ::
    SEMICOLON,// ;
    DOT,// .

    keywordBeg,// ALL and the following are InfluxQL Keywords
    ALL,
    ALTER,
    ANALYZE,
    ANY,
    AS,
    ASC,
    BEGIN,
    BY,
    CARDINALITY,
    CREATE,
    CONTINUOUS,
    DATABASE,
    DATABASES,
    DEFAULT,
    DELETE,
    DESC,
    DESTINATIONS,
    DIAGNOSTICS,
    DISTINCT,
    DROP,
    DURATION,
    END,
    EVERY,
    EXACT,
    EXPLAIN,
    FIELD,
    FOR,
    FROM,
    GRANT,
    GRANTS,
    GROUP,
    GROUPS,
    IN,
    INF,
    INSERT,
    INTO,
    KEY,
    KEYS,
    KILL,
    LIMIT,
    MEASUREMENT,
    MEASUREMENTS,
    NAME,
    OFFSET,
    ON,
    ORDER,
    PASSWORD,
    POLICY,
    POLICIES,
    PRIVILEGES,
    QUERIES,
    QUERY,
    READ,
    REPLICATION,
    RESAMPLE,
    RETENTION,
    REVOKE,
    SELECT,
    SERIES,
    SET,
    SHOW,
    SHARD,
    SHARDS,
    SLIMIT,
    SOFFSET,
    STATS,
    SUBSCRIPTION,
    SUBSCRIPTIONS,
    TAG,
    TO,
    USER,
    USERS,
    VALUES,
    WHERE,
    WITH,
    WRITE,
    keywordEnd;


    String operate;

    Token(String operate) {
        this.operate = operate;
    }

    Token() {

    }

    public String getOperate() {
        return this.operate;
    }

}
