package com.godalgo.inspirer.cloud.analytics.aop;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.godalgo.inspirer.cloud.analytics.handler.BaseHandler;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.context.Context;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by 杭盖 on 2018/8/23.
 */
public final class ContextFactory {

  static public void step(BaseHandler handler, Context context, String jsonString) {
    JSONObject params = !jsonString.isEmpty() ? JSON.parseObject(jsonString) : new JSONObject();
    HttpServletRequest request = handler.getRequest();
    List<String> rawGroupList = handler.getRawGroupList();

    List<String> rest = Arrays.asList(request.getParameterValues("rest"));
    HashMap<String, String> hash = new HashMap();

    if (rest != null) {
      hash.put("array", String.format("ARRAY%s", JSON.toJSONString(rest, SerializerFeature.UseSingleQuotes)));
      hash.put("join", String.join(",", rest));
    }
    ArrayList<String> restJoins = new ArrayList();
    String joins = "";
    for (String step : rest) {
      joins += joins.isEmpty() ? step : ("," + step);
      restJoins.add(joins);
    }

    List<String> groupAlias = new ArrayList();
    List<String> viewAlias = new ArrayList();
    List<String> groupAggs = new ArrayList();
    ArrayList<String> dynamicColumns = new ArrayList();
    ArrayList<String> dynamicColumnsWithType = new ArrayList();
    ArrayList<String> viewDynamicColumns = new ArrayList();
    ArrayList<String> viewDynamicColumnsWithAlias = new ArrayList();
    ArrayList<String> viewDynamicColumnsWithType = new ArrayList();
    String aggFormat = "LAST_VALUE (%s) WITHIN GROUP (ORDER BY %s ASC) %s";
    for (int i = 0; i < rawGroupList.size(); i++) {
      String rawGroup = rawGroupList.get(i);
      String[] columnSegs = handler.getDynamicColumn(rawGroup);
      String column = columnSegs[1];
      String type = columnSegs[2];

      String as = "AS" + i;
      dynamicColumns.add(i, column + " " + as);
      dynamicColumnsWithType.add(i, column + " " + type);

      String alias = "GROUP" + i;
      groupAggs.add(String.format(aggFormat, as, params.getString("orderBy"), alias));
      groupAlias.add(alias);

      // STEPS_VIEW的动态元素都归属于GROUPBY family
      String viewDynamicColumn = "GROUPBY." + alias;
      // 中间视图来源于derived table, 所以as可以复用(不同SQL)
      viewDynamicColumns.add(i, viewDynamicColumn);
      viewDynamicColumnsWithAlias.add(i, viewDynamicColumn + " " + as);
      viewDynamicColumnsWithType.add(i, viewDynamicColumn + " " + type);
      viewAlias.add(as);
    }

    String viewAliasPrefix = params.getString("viewAliasPrefix");
    if (viewAliasPrefix != null) {
      for (int i = 0; i < viewAlias.size(); i++) {
        viewAlias.set(i, viewAliasPrefix + "." + viewAlias.get(i));
      }
    }

    context.put("dynamicColumns", !dynamicColumns.isEmpty() ? ", " + String.join(",", dynamicColumns) : "");
    context.put("dynamicColumnsWithType", !dynamicColumnsWithType.isEmpty() ? ", " + String.join(",", dynamicColumnsWithType) : "");
    context.put("groupAlias", !groupAlias.isEmpty() ? ", " + String.join(",", groupAlias) : "");
    context.put("groupAggs", !groupAggs.isEmpty() ? ", " + String.join(",", groupAggs) : "");

    context.put("viewAlias", !viewAlias.isEmpty() ? ", " + String.join(",", viewAlias) : "");
    context.put("viewDynamicColumns", !viewDynamicColumns.isEmpty() ? ", " + String.join(",", viewDynamicColumns) : "");
    context.put("viewDynamicColumnsWithAlias", !viewDynamicColumnsWithAlias.isEmpty() ? ", " + String.join(",", viewDynamicColumnsWithAlias) : "");
    context.put("viewDynamicColumnsWithType", !viewDynamicColumnsWithType.isEmpty() ? ", " + String.join(",", viewDynamicColumnsWithType) : "");

    context.put("rest", hash);
    context.put("restSize", rest.size());
    context.put("restJoins", restJoins);
  }

  static public void indicator(BaseHandler handler, Context context, String jsonString) throws Exception {
    HttpServletRequest request = handler.getRequest();
    List<String> rawGroupList = handler.getRawGroupList();
    JSONObject divisor = JSON.parseObject(request.getParameter("divisor"));

    context.put("dividend", handler.getLambda(request.getParameter("select")));
    context.put("divisor", handler.getLambda(divisor.getString("select"), divisor));

    ArrayList<String> dynamicColumns = handler.getAllDynamicColumns(false, true);
    context.put("dynamicColumns", !dynamicColumns.isEmpty() ? ", " + String.join(",", dynamicColumns) : "");



  }


}
