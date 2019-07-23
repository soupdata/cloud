package com.godalgo.inspirer.cloud.analytics.annoation;

/**
 * Created by 杭盖 on 2018/7/6.
 * 额外参数
 */


import org.springframework.data.jpa.repository.Query;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Querys {
  Query[] value();
}