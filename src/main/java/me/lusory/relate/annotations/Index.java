package me.lusory.relate.annotations;

import java.lang.annotation.*;

/**
 * Define an index on a table.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Repeatable(Indexes.class)
public @interface Index {

    String name();

    String[] properties();

    boolean unique() default false;
}
