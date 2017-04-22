package com.infomaximum.rocksdb.core.anotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Created by user on 19.04.2017.
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface Entity {

    String columnFamily();
}
