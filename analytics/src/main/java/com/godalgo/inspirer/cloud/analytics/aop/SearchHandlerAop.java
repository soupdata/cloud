package com.godalgo.inspirer.cloud.analytics.aop;

import com.godalgo.inspirer.cloud.analytics.annoation.AddArg;
import com.godalgo.inspirer.cloud.analytics.annoation.GenericDomain;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by 杭盖 on 2018/7/5.
 */
@Component
@Aspect
public class SearchHandlerAop extends BaseAop {

  @Pointcut(value = "execution(public * com.godalgo.inspirer.cloud.analytics.handler.SearchHandler.*(..))")
  private void parse() {}

  @Around(value = "parse() && args(request,nativeSql)")
  public ArrayList<HashMap> withUrl(ProceedingJoinPoint joinPoint, HttpServletRequest request, String nativeSql) throws Throwable {
    System.out.println("Before JoinPoint for " + joinPoint.toString());
    Method method = getMethod(joinPoint);
    Object result = executeNativeSql(joinPoint, request, nativeSql, method);
    System.out.println("After JoinPoint for " + joinPoint.toString());

    AddArg arg = method.getDeclaredAnnotation(AddArg.class);
    String[] keys = arg.value().split(",");

    List<HashMap> rs = (List<HashMap>) result;
    HashMap<String, ArrayList<HashMap>> map = new HashMap();
    ArrayList<HashMap> list = new ArrayList();

    int len = rs.size();
    for (int i = 0; i < len; i++) {
      HashMap row = rs.get(i);
      String pageKey = getGroupKey(keys, row);
      ArrayList<HashMap> page = map.get(pageKey);
      if (page == null) {
        page = new ArrayList();
        map.put(pageKey, page);
        HashMap<String, Object> hash = new HashMap();
        hash.put("ID", pageKey);
        String title = (String) row.get("TITLE");
        if (title != null) hash.put("TITLE", title);
        hash.put("ELEMENTS", page);
        list.add(hash);
      }
      page.add(row);
    }

    return list;
  }

  private String getGroupKey(String[] keys, HashMap row) {
    String groupKey = "";
    for (int i = 0; i < keys.length; i++) {
      groupKey += (String) row.get(keys[i]);
    }
    return groupKey;
  }
}
