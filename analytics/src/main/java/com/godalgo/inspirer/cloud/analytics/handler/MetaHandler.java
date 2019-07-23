package com.godalgo.inspirer.cloud.analytics.handler;

/**
 * Created by 杭盖 on 2018/1/3.
 */


import com.godalgo.inspirer.cloud.analytics.annoation.AddI18N;
import com.godalgo.inspirer.cloud.analytics.annoation.GenericDomain;
import com.godalgo.inspirer.cloud.analytics.annoation.I18N;
import com.godalgo.inspirer.cloud.analytics.annoation.Lang;
import com.godalgo.inspirer.cloud.analytics.domain.EventCession;
import com.godalgo.inspirer.cloud.analytics.repository.EventCessionRepository;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import com.vladmihalcea.hibernate.type.array.internal.ArrayUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

@RestController
@RequestMapping("/do/meta/search")
public class MetaHandler extends BaseHandler<EventCession> {
  @Autowired
  private QueryImpl doQuery;

  @Autowired
  MedusaHandler medusa;

  // 视图层默认不显示的字段, 根据实际情况决定是否需要配置到数据库
  private final static List<String> reservedQualifies = Arrays.asList(
    "ga_Target", "ga_TargetXpath", "ga_Selector", "ga_Xpath",
    "ga_EventNo", "ga_TimerId", "ga_TraceId", "ga_ErrorLineNumber",
    "ga_Version", "ga_Bytes", "ga_Method", "ga_RequestTime",
    "ga_StatusCode", "ga_CacheId", "ga_PageId"
  );
  private final static List<String> filteredFamily = Arrays.asList(
    "performance", "thrown", "event"
  );

  //

  private final static String REG_SYSTEM_QUALIFY_PREFIX = "^ga_";

  private final static List<String> lambdaList = Arrays.asList("SUM", "AVG", "MAX", "MIN");
  private final static BigDecimal aggregate(String lambda, List<BigDecimal> list) {
    BigDecimal number = null;
    switch (lambda) {
      case "MAX":
        number = Collections.max(list);
        break;
      case "MIN":
        number = Collections.min(list);
        break;
      case "SUM":
        number = BigDecimal.valueOf(list.stream().mapToDouble(BigDecimal::doubleValue).sum());
        break;
      case "AVG":
        number = BigDecimal.valueOf(list.stream().mapToDouble(BigDecimal::doubleValue).average().getAsDouble());
        break;
    }
    return number;
  }


/*  @RequestMapping(value = "/", method = RequestMethod.GET)
  public ModelAndView index(HttpServletResponse response,
                            HttpServletRequest request) throws MalformedURLException {
    return new ModelAndView("index");

  }*/

  // 获取收藏列表
  @RequestMapping(value = "/getFavorites", method = RequestMethod.GET)
  @Query(value = "SELECT * FROM FAVORITES WHERE #{domainCondition}", nativeQuery = true)
  public @ResponseBody
  HashMap<String, List<HashMap>> getFavorites(HttpServletRequest request, String nativeSql) {
    HashMap<String, List<HashMap>> hash = new HashMap();
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    int len = rs.size();
    while (len-- > 0) {
      HashMap row = rs.get(len);
      String type = (String) row.get("TYPE");
      if (!hash.containsKey(type)) hash.put(type, new ArrayList());
      hash.get(type).add(row);
    }
    return hash;
  }

  // 获取指标列表
  @RequestMapping(value = "/getIndexes", method = RequestMethod.GET)
  public @ResponseBody
  List<String[]> getIndexes(HttpServletRequest request) {

    List<String[]> indexes = new ArrayList();
    Method[] methods = EventCessionRepository.class.getDeclaredMethods();
    for (Method method : methods) {
      I18N annoI18n = method.getDeclaredAnnotation(I18N.class);
      Lang annoLang = method.getDeclaredAnnotation(Lang.class);
      String lang = annoLang == null ? "cn" : annoLang.value();
      if (annoI18n != null) {
        for (AddI18N i18n : annoI18n.value()) {
          if (i18n.lang().equals(lang)) indexes.add(new String[]{i18n.value(), i18n.code()});
        }
      }
    }
    return indexes;
  }

  @RequestMapping(value = "/getQualifies", method = RequestMethod.GET)
  public @ResponseBody
  HashMap requestQualifies(HttpServletRequest request, HttpServletResponse response,
                     @RequestParam(value = "steps", required = true) List<JSONObject> steps

  ) throws SQLException {
    HashMap injection = new HashMap();
    injection.put("steps", steps);
    request.setAttribute("context", injection);
    return ((MetaHandler) AopContext.currentProxy()).getQualifies(request, null);
  }
  // 从前端传过来的这四个参数 eventName nodeText selector xpath 永远不会同时为空, 所以UNION ALL CESSION, PAGE FAMILY and GLOBAL FAMILY(EVENTS.0)
  // 若要在全站范围内统计CESSION,PAGE列簇(设计为页面范围内), 需要仔细斟酌此场景的适用性
  // ajax列簇的page_url为ajax请求地址
  // 一个漏斗即一组事件, 一个事件即只有一步的漏斗
  @Query(value =
    "#foreach ($step in $steps) " +
      "#if($velocityCount!=1) UNION ALL #end " +
      "SELECT NAME,TAB,FAMILY,TYPE,CALC,PAGE_URL,DESCRIPTOR FROM QUALIFY_METAS " +
      "WHERE #{domainCondition} " +
      "#if(!$step.optString('pageUrl').equals('')) AND page_url='#{step.optString('pageUrl')}' #end " +
      "#if(!$step.optString('eventName').equals('')) AND event_name='#{step.optString('eventName')}' #end " +
      "#if(!$step.optString('nodeText').equals('')) AND node_text='#{step.optString('nodeText')}' #end " +
      "#if(!$step.optString('selector').equals('')) AND selector='#{step.optString('selector')}' #end " +
      "#if(!$step.optString('xpath').equals('')) AND xpath='#{step.optString('xpath')}' #end " +
      "UNION ALL " +
      "SELECT NAME,TAB,FAMILY,TYPE,CALC,PAGE_URL,DESCRIPTOR FROM QUALIFY_METAS " +
      "WHERE #{domainCondition} AND event_name IS NULL AND node_text IS NULL AND selector IS NULL AND xpath IS NULL AND " +
      "(page_url IS NULL #if(!$step.optString('pageUrl').equals('')) OR page_url='#{step.optString('pageUrl')}' #end) " +
      "UNION ALL " +
      "SELECT NAME,TAB,FAMILY,TYPE,CALC,PAGE_URL,DESCRIPTOR FROM QUALIFY_METAS " +
      "WHERE #{domainCondition} AND FAMILY='ajax' " +
      "#if(!$step.optString('eventName').equals('')) AND event_name='#{step.optString('eventName')}' #end " +
      "#if(!$step.optString('nodeText').equals('')) AND node_text='#{step.optString('nodeText')}' #end " +
      "#if(!$step.optString('selector').equals('')) AND selector='#{step.optString('selector')}' #end " +
      "#if(!$step.optString('xpath').equals('')) AND xpath='#{step.optString('xpath')}' #end " +
    "#end LIMIT 300", nativeQuery = true)
  @GenericDomain(enable = true)
  public HashMap getQualifies(HttpServletRequest request, String nativeSql) throws SQLException {
    // 获取
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    HashMap<String, HashMap<String, ArrayList<String>>> values = new HashMap();
    int len = rs.size();
    // GROUP BY TAB,FAMILY, 不包含PAGE_URL(全局模式与GROUP BY PAGE_URL相斥)
    while (--len >= 0) {
      Map<String, ?> map = rs.get(len);
      String table = map.get("TAB").toString();
      String family = map.get("FAMILY").toString();
      String key = table + "-" + family;

      addValues(values, key, "names", map.get("NAME").toString());
      addValues(values, key, "types", map.get("TYPE").toString());
      addValues(values, key, "urls", map.get("PAGE_URL") == null ? "" : map.get("PAGE_URL").toString());
      addValues(values, key, "calcs", map.get("CALC") == null ? "" : map.get("CALC").toString());
      addValues(values, key, "descriptors", map.get("DESCRIPTOR") == null ? "" : map.get("DESCRIPTOR").toString());
    }

    HashMap<String, HashMap<String, String[]>> hash = new HashMap();
    HashMap<String, String> i18n = new HashMap();

    for (Map.Entry<String, HashMap<String, ArrayList<String>>> entry : values.entrySet()) {
      HashMap<String, ArrayList<String>> map = entry.getValue();
      String[] splits = entry.getKey().split("-");
      String table = splits[0];
      String family = splits[1];
      String key = table + "." + family;

      if (filteredFamily.contains(family)) continue;
      // 若无翻译, 使用Capitalize值
      i18n.put(key, StringUtils.capitalize(family));
      hash.put(key, new HashMap());
      ArrayList<String> qualifies = map.get("names");
      ArrayList<String> types = map.get("types");
      ArrayList<String> calcs = map.get("calcs");
      ArrayList<String> urls = map.get("urls");
      ArrayList<String> descriptors = map.get("descriptors");

      // 按照reservedQualifies过滤字段
      int i = 0;
      for (String qualify : qualifies) {
        String type = types.get(i++);
        String calc = "";
        if (calcs != null) calc = calcs.get(i - 1);
        if (calc == null) calc = "";

        String url = "";
        if (urls != null) url = urls.get(i - 1);
        if (url == null) url = "";

        String descriptor = "";
        if (descriptors != null) descriptor = descriptors.get(i - 1);
        if (descriptor == null) descriptor = "";
        if (!reservedQualifies.contains(qualify)) {
          hash.get(key).put(qualify, new String[]{type, calc, url, descriptor});
          i18n.put(qualify, StringUtils.capitalize(qualify.replaceFirst(REG_SYSTEM_QUALIFY_PREFIX, "").replace("_", " ").replace("AjaxData ", "")));
        }
      }
    }

    // 翻译qualify
    ArrayList<String> array = new ArrayList();
    for (Map.Entry<String, String> entry : i18n.entrySet()) {
      array.add("'" + entry.getKey() + "'");
    }

    String sql = String.format("SELECT NAME,FIRST_VALUE(TEXT) WITHIN GROUP (ORDER BY DOMAIN DESC) AS TEXT FROM DICTIONARIES " +
      "WHERE #{domainCondition} " +
      "AND NAMESPACE='QUALIFY_METAS' " +
      "AND LANG='#{lang}' " +
      "AND NAME=ANY(ARRAY[%s]) " +
      "GROUP BY NAME", String.join(",", array));
    List<HashMap<String, String>> translates = (List<HashMap<String, String>>) medusa.translate(request, sql);

    for (HashMap<String, String> entry : translates) {
      i18n.put(entry.get("NAME"), entry.get("TEXT"));
    }
    return new HashMap() {{
      put("hash", hash);
      put("i18n", i18n);
    }};
  }


  // 获取维度区间值
  @RequestMapping(value = "/getRange", method = RequestMethod.GET)
  @Query(value = "SELECT MIN(#{column}) AS MIN, MAX(#{column}) AS MAX FROM #{table}(#{column} DECIMAL)", nativeQuery = true)
  public @ResponseBody
  HashMap getRange(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs.get(0);
  }

  private HashMap getArrayRange(HttpServletRequest request, List<HashMap> rs) throws Exception {
    String lambda = request.getParameter("lambda");
    if (lambda == null || !lambdaList.contains(lambda)) throw new Exception("Illegal parameter value");
    lambda = lambda.toUpperCase();

    List<BigDecimal> list = new ArrayList();
    for (int i = 0; i < rs.size(); i++) {
      BigDecimal[] array = (BigDecimal[]) rs.get(i).get("COL");

      list.add(aggregate(lambda, Arrays.asList(array)));
    }
    BigDecimal max = aggregate("MAX", list);
    BigDecimal min = aggregate("MIN", list);
    HashMap<String, BigDecimal> hash = new HashMap();
    hash.put("MAX", max);
    hash.put("MIN", min);
    return hash;
  }

  // 参考值意即可能非完整, 仅供参考
  @RequestMapping(value = "/getArrayRange", method = RequestMethod.GET)
  @Query(value = "SELECT #{column} AS COL FROM #{table}(#{column} DECIMAL[]) " +
    "WHERE #{column} IS NOT NULL LIMIT 10", nativeQuery = true)
  public @ResponseBody
  HashMap getArrayRange(HttpServletRequest request, String nativeSql) throws Exception {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return getArrayRange(request, rs);
  }


  // 获取维度枚举参考值, 最大支持24个成员
  @RequestMapping(value = "/getEnum", method = RequestMethod.GET)
  @Query(value = "SELECT FIRST_VALUES(COL, 24) WITHIN GROUP (ORDER BY COL ASC) AS ENUMS FROM " +
    "(SELECT #{column} AS COL FROM #{table}(#{column} VARCHAR) " +
    "GROUP BY COL ORDER BY COL ASC LIMIT 24)", nativeQuery = true)
  public @ResponseBody
  String[] getEnum(HttpServletRequest request, String nativeSql) {

    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return (String[]) rs.get(0).get("ENUMS");
  }

  private List<String> getArrayEnum(HttpServletRequest request, List<HashMap> rs) {
    List<String> enums = new ArrayList();
    for (int i = 0; i < rs.size(); i++) {
      String[] array = (String[]) rs.get(i).get("COL");
      enums.addAll(Arrays.asList(array));
    }
    return enums;
  }

  // Phoenix的dynamic column使用`VARCHAR ARRAY`类型, 会抛出`java.lang.OutOfMemoryError: Java heap space`
  @RequestMapping(value = "/getArrayEnum", method = RequestMethod.GET)
  @Query(value = "SELECT #{column} AS COL FROM #{table}(#{column} VARCHAR[]) " +
    "WHERE #{column} IS NOT NULL LIMIT 10", nativeQuery = true)
  public @ResponseBody
  List<String> getArrayEnum(HttpServletRequest request, String nativeSql) {

    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return getArrayEnum(request, rs);
  }

  // 内部(ga_)type为NUMBER的`ENUM LIKE`字段专属
  @RequestMapping(value = "/getNumberEnum", method = RequestMethod.GET)
  @Query(value = "SELECT FIRST_VALUES(COL, 24) WITHIN GROUP (ORDER BY COL ASC) AS ENUMS FROM " +
    "(SELECT #{column} AS COL FROM #{table}(#{column} DECIMAL) " +
    "GROUP BY COL ORDER BY COL ASC LIMIT 24)", nativeQuery = true)
  public @ResponseBody
  BigDecimal[] getNumberEnum(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return (BigDecimal[]) rs.get(0).get("ENUMS");
  }

  // 获取维度SET参考值, 最大支持4个成员, 理论上讲SET是ENUM子集
  @RequestMapping(value = "/getSet", method = RequestMethod.GET)
  @Query(value = "SELECT FIRST_VALUES(COL, 24) WITHIN GROUP (ORDER BY COL ASC) AS SETS FROM " +
    "(SELECT #{column} AS COL FROM #{table}(#{column} VARCHAR) GROUP BY COL ORDER BY COL ASC LIMIT 24)", nativeQuery = true)
  public @ResponseBody
  List<String> getSet(HttpServletRequest request, String nativeSql) {
    List<String> sets = new ArrayList();
    List<HashMap> rs = doQuery.queryBySql(nativeSql);

    String[] column = (String[]) rs.get(0).get("SETS");
    int len = column.length;
    for (int i = 0; i < len; i++) {
      String set = column[i];
      sets.addAll(Arrays.asList(set.split(",")));
    }

    return sets;
  }

  @RequestMapping(value = "/getJsonRange", method = RequestMethod.GET)
  @Query(value = "SELECT MAX(MAX) AS MAX, MIN(MIN) AS MIN FROM (" +
    "SELECT JSON_ELEM_AGG(#{column}, '#{param.get('key')}', 'DECIMAL', 'MAX') AS MAX, " +
    "JSON_ELEM_AGG(#{column},'#{param.get('key')}','DECIMAL','MIN') AS MIN " +
    "FROM #{table}(#{column} VARCHAR)" +
    ")", nativeQuery = true)
  public @ResponseBody
  HashMap getJsonRange(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs.get(0);
  }

  @RequestMapping(value = "/getJsonEnum", method = RequestMethod.GET)
  @Query(value = "SELECT FIRST_VALUES(COL, 24) WITHIN GROUP (ORDER BY COL ASC) AS ENUMS FROM (" +
    "SELECT JSON_ELEM(#{column},'#{param.get('key')}','VARCHAR') AS COL " +
    "FROM #{table}(#{column} VARCHAR) " +
    "GROUP BY COL ORDER BY COL ASC LIMIT 24" +
    ")", nativeQuery = true)
  public @ResponseBody
  String[] getJsonEnum(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return (String[]) rs.get(0).get("ENUMS");
  }

  @RequestMapping(value = "/getJsonArrayEnum", method = RequestMethod.GET)
  @Query(value = "SELECT JSON_ELEM_ARRAY(#{column},'#{param.get('key')}','VARCHAR','DISTINCT') AS COL FROM #{table}(#{column} VARCHAR) " +
    "WHERE #{column} IS NOT NULL LIMIT 10", nativeQuery = true)
  public @ResponseBody
  List<String> getJsonArrayEnum(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return getArrayEnum(request, rs);
  }

  // 参考值意即可能非完整, 仅供参考
  @RequestMapping(value = "/getJsonArrayRange", method = RequestMethod.GET)
  @Query(value = "SELECT JSON_ELEM_ARRAY(#{column},'#{param.get('key')}','DECIMAL','DISTINCT') AS COL FROM #{table}(#{column} VARCHAR) " +
    "WHERE #{column} IS NOT NULL LIMIT 10", nativeQuery = true)
  public @ResponseBody
  HashMap getJsonArrayRange(HttpServletRequest request, String nativeSql) throws Exception {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return getArrayRange(request, rs);
  }

  // {"hash":{"EVENTS.0":{"name":"Number","sid":"Number"}},"i18n":{}}
  private HashMap<String, HashMap<String, ArrayList<String>>> addValues(HashMap<String, HashMap<String, ArrayList<String>>> values, String key, String groupKey, String name) {
    if (!values.containsKey(key)) values.put(key, new HashMap());
    HashMap<String, ArrayList<String>> value = values.get(key);
    if (!value.containsKey(groupKey)) value.put(groupKey, new ArrayList());
    value.get(groupKey).add(name);
    return values;
  }
}
