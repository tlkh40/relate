package net.lecousin.reactive.data.relational.annotations;

import org.springframework.data.relational.core.mapping.Column;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allow to specify information about a column to generate the schema.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Column
public @interface ColumnDefinition {

    /**
     * Defines is the column may contain NULL or not.
     */
    boolean nullable() default true;

    /**
     * Defines if the value may be updated or not.
     */
    boolean updatable() default true;

    /**
     * Minimum value or length.
     */
    long min() default 0;

    /**
     * Maximum value or length.
     */
    long max() default -1;

    /**
     * Floating-point precision.
     */
    int precision() default -1;

    /**
     * Floating-point scale.
     */
    int scale() default -1;
}
