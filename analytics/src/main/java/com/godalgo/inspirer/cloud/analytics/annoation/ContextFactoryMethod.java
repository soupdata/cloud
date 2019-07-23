package com.godalgo.inspirer.cloud.analytics.annoation;

/**
 * Created by 杭盖 on 2018/7/6.
 * 额外参数
 */

import java.lang.annotation.*;
import java.util.List;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ContextFactoryMethod {
  String method() default "";
  String params() default "";
}