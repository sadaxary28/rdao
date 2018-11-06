package com.infomaximum.database.exception.runtime;

public class UnsupportedTypeException extends IllegalTypeException {

    public UnsupportedTypeException(Class type) {
        super("Unsupported type " + type);
    }
}
