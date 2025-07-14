#pragma once

class DataType {
public:
    static const int BOOLEAN = 16;
    static const int TINYINT = -6;
    static const int SMALLINT = 5;
    static const int INTEGER = 4;
    static const int BIGINT = -5;
    static const int FLOAT = 6;
    static const int DOUBLE = 8;
    static const int DECIMAL = 3;
    static const int NUMERIC = 2;
    static const int DATE = 91;
    static const int TIMESTAMP = 93;
    static const int TIMESTAMP_WITH_TIMEZONE = 2014;
    static const int VARCHAR = 12;
    static const int BINARY = -2;
    static const int BLOB = 2004;
    static const int CLOB = 2005;
    static const int NCLOB = 2011;
    static const int SQLXML = 2009;
    static const int JSON = 1111;
};
