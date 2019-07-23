package com.godalgo.inspirer.cloud.analytics;

import com.vladmihalcea.hibernate.type.array.internal.ArraySqlTypeDescriptor;
import com.vladmihalcea.hibernate.type.array.internal.StringArrayTypeDescriptor;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.usertype.DynamicParameterizedType;

import java.util.Properties;

/**
 * Created by 杭盖 on 2018/6/4.
 */
public class StringArrayType
  extends AbstractSingleColumnStandardBasicType<String[]>
  implements DynamicParameterizedType {

  public static final com.vladmihalcea.hibernate.type.array.StringArrayType INSTANCE = new com.vladmihalcea.hibernate.type.array.StringArrayType();

  public StringArrayType() {
    super(ArraySqlTypeDescriptor.INSTANCE, StringArrayTypeDescriptor.INSTANCE);
  }

  public String getName() {
    return "string-array";
  }

  @Override
  protected boolean registerUnderJavaType() {
    return true;
  }

  @Override
  public void setParameterValues(Properties parameters) {
    if (!parameters.isEmpty())
      ((StringArrayTypeDescriptor) getJavaTypeDescriptor()).setParameterValues(parameters);
  }
}
