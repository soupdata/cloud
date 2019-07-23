package com.godalgo.inspirer.cloud.analytics.annoation;

import java.lang.annotation.*;

/**
 * Created by 杭盖 on 2018/4/26.
 * 某些查询需要基于全量数据
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Backdate {
  boolean enable() default false;
}
