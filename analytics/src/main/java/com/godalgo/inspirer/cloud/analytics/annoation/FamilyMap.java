package com.godalgo.inspirer.cloud.analytics.annoation;

/**
 * Created by 杭盖 on 2018/08/23.
 */

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FamilyMap {
  String table() default "";
  String family() default "";
}