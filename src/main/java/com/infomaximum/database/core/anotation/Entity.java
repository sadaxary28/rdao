package com.infomaximum.database.core.anotation;

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

    String name();

    Field[] fields();

    /**
     * (Optional) Indexes for the table. These are only used if table generation is in effect.  Defaults to no
     * additional indexes.
     *
     * @return The indexes
     */
    Index[] indexes() default {};
    PrefixIndex[] prefixIndexes() default {};
}
