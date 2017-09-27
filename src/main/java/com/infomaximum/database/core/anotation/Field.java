package com.infomaximum.database.core.anotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Created by user on 19.04.2017.
 */
@Target({})
@Retention(RUNTIME)
public @interface Field {

    String name();
    Class type();
}