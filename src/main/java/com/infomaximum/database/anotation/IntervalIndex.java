package com.infomaximum.database.anotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({})
@Retention(RUNTIME)
public @interface IntervalIndex {

    /**
     * Supported Integer, Double, Date types only.
     */
    String indexedField();

    String[] hashedFields() default {};
}
