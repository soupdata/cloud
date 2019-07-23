package com.godalgo.inspirer.cloud.analytics.annoation;

/**
 * Created by 杭盖 on 2018/4/22.
 */

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AddDynamicColumns {
  AddDynamicColumn[] value();
}