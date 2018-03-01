package com.infomaximum.database.anotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target(TYPE)
@Retention(RUNTIME)
public @interface Entity {

    String namespace();
    String name();
    Field[] fields();

    Index[] indexes() default {};
    Index[] prefixIndexes() default {};
    IntervalIndex[] intervalIndexes() default {};
}
