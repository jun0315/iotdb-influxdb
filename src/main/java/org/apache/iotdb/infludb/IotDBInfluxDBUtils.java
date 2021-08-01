package org.apache.iotdb.infludb;

public final class IotDBInfluxDBUtils {
    public static String checkNonEmptyString(String string, String name) throws IllegalArgumentException {
        if (string != null && !string.isEmpty()) {
            return string;
        } else {
            throw new IllegalArgumentException("Expecting a non-empty string for " + name);
        }
    }
}
