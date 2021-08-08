package org.apache.iotdb.infludb.influxql;

public enum DataType {
    // Unknown primitive data type.
    Unknown,
    // Float means the data type is a float.
    Float,
    // Integer means the data type is an integer.
    Integer,
    // String means the data type is a string of text.
    String,
    // Boolean means the data type is a boolean.
    Boolean,
    // Time means the data type is a time.
    Time,
    // Duration means the data type is a duration of time.
    Duration,
    // Tag means the data type is a tag.
    Tag,
    // AnyField means the data type is any field.
    AnyField,
    // Unsigned means the data type is an unsigned integer.
    Unsigned,

}
