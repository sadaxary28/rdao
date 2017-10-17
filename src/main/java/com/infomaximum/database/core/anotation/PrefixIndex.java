package com.infomaximum.database.core.anotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * https://confluence.office.infomaximum.com/pages/viewpage.action?pageId=2432046
 */
@Target({})
@Retention(RUNTIME)
public @interface PrefixIndex {

    String name();
}
