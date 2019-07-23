package com.godalgo.inspirer.cloud.analytics.annoation;

import java.lang.annotation.*;

/**
 * Created by 杭盖 on 2018/4/26.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface View {
  String value() default "";
}
