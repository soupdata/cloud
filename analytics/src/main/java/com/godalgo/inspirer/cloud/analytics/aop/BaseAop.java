package com.godalgo.inspirer.cloud.analytics.aop;

import com.godalgo.inspirer.cloud.analytics.annoation.GenericDomain;
import org.apache.commons.lang3.StringUtils;
import org.apache.hbase.thirdparty.com.google.protobuf.MapEntry;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.data.jpa.repository.Query;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 杭盖 on 2018/6/8.
 */
public class BaseAop {
  protected boolean authenticate(HttpServletRequest request) {
    // accessKey 是否已登录 -> 组织列表 -> 域列表
    HttpSession session = request.getSession();
    session.setAttribute("domain", ".youku.com");
    return true;
  }

  protected Context getContext(HttpServletRequest request, Method method) throws Exception {
    if (!authenticate(request)) throw new Exception("Authentication failure.");

    Context context = new VelocityContext();
    HashMap<String, String> param = new HashMap<>();
    Map<String, String[]> params = request.getParameterMap();
    for (Map.Entry<String, String[]> item : params.entrySet()) {
      String key = item.getKey();
      String[] values = item.getValue();
      param.put(key, String.join(",", values));
    }
    context.put("param", param);

    GenericDomain genericAnno = method.getDeclaredAnnotation(GenericDomain.class);
    boolean isGenericDomain = false;
    if (genericAnno != null) isGenericDomain = genericAnno.enable();
    Object domainAttr = request.getSession().getAttribute("domain");
    String domain = domainAttr == null ? "" : domainAttr.toString();
    ArrayList<String> domains = new ArrayList();
    // 配合LIKE使用
    if (isGenericDomain) {
      domain = getTopLevelDomain(domain);
      domains.add("DOMAIN='*'");
    }
    if (!domain.isEmpty()) domains.add("DOMAIN LIKE '%" + domain + "'");

    context.put("domainCondition", "(" + String.join(" OR ", domains) + ")");
    context.put("domain", domain);

    Object lang = request.getSession().getAttribute("lang");
    context.put("lang", lang == null ? "cn" : lang.toString());

    String select = request.getParameter("select");
    if (!StringUtils.isEmpty(select)) {
      String[] splits = select.split("\\.", 2);
      context.put("table", splits[0]);
      context.put("column", splits[1]);
    }

    HashMap<String, Object> injection = (HashMap) request.getAttribute("context");
    if (injection != null) {
      for (Map.Entry<String, Object> elem : injection.entrySet()) {
        context.put(elem.getKey(), elem.getValue());
      }
    }
    request.removeAttribute("context");

    return context;
  }


  protected StringWriter getStringWriter(Context context, String instring) {
    StringWriter sw = new StringWriter();
    // 使用#号为了保持@Query的语法高亮，不建议改动Velocity默认变量标识符
    // TODO: 模板预编译
    instring = instring.replaceAll("#\\{","\\$\\{");
    Velocity.evaluate(context, sw, "Velocity", instring);
    return sw;
  }

  protected Object executeNativeSql(ProceedingJoinPoint joinPoint, HttpServletRequest request, String nativeSql, Method method) throws Throwable {
    Object[] args = joinPoint.getArgs();
    String query = nativeSql;
    if (query == null) query = method.getDeclaredAnnotation(Query.class).value();
    Context context = getContext(request, method);
    StringWriter sw = getStringWriter(context, query);

    args[1] = sw.getBuffer().toString();
    Object result = joinPoint.proceed(args);
    return result;
  }

  protected Method getMethod(ProceedingJoinPoint joinPoint) throws NoSuchMethodException {
    String joinPointName = joinPoint.getSignature().getName();
    Method method = joinPoint.getTarget().getClass().getDeclaredMethod(joinPointName, HttpServletRequest.class, String.class);
    return method;
  }

  // www.godalgo.com:8888 -> .godalgo.com:8888
  private String getTopLevelDomain(String domain) {
    if (domain.isEmpty()) return domain;
    String[] segs = domain.split("\\.");
    int len = segs.length;
    String[] splits = segs[len - 1].split("\\:");
    String port = (splits.length == 1) ? "" : ":" + splits[1];
    return "." +  segs[len - 2] + "." + splits[0] + port;
  }
}
