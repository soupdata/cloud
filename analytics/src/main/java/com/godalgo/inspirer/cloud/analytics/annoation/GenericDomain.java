package com.godalgo.inspirer.cloud.analytics.annoation;

import java.lang.annotation.*;

/**
 * Created by 杭盖 on 2018/4/26.
 * 泛域名: www.godalgo.com -> .godalgo.com -> *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface GenericDomain {
  boolean enable() default false;
}
