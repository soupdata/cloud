package com.godalgo.inspirer.cloud.analytics.handler;

import com.godalgo.inspirer.cloud.analytics.AnalyticsApp;

import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;

import com.sun.jersey.core.impl.provider.entity.Inflector;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.util.ReflectionUtils;

import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.PrimaryKeyJoinColumns;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.*;

import static java.util.Arrays.*;

/**
 * 历史背景: 先写了这个类, 后接触了 Criterial 标准
 * TODO: 重构, 使之遵循 Criterial 标准 (扩展 Criterial 对 `Phoenix Dynamic Columns` & Phoenix Special SQL 的支持)
 * Created by 杭盖 on 2018/1/3.
 */

public abstract class BaseHandler<T> {
  private static String DOMAIN_PACKAGE = "com.godalgo.inspirer.cloud.analytics.domain";
  private final String actualTypeName = "EVENTS";

  // private static final String TABLE_PREFIX = "inspirer.";
  // Phoenix字段超过4段（schema、table、family、qualify）会报语法错误
  // 开启USE inspirer可以省去schema段，但在多表（包括跨schema场景）时仍然需要显示声明
  // 本质上无法回避此BUG，可动态生成别名解决
  private static final String TABLE_PREFIX = "";
  private static final String COLUMN_SPLITOR = "\\.";

  // 默认支持的指标
  // COUNT, SUM, AVG, DISTINCT, MAX, MIN 分别对应总次数、总值、平均值、去重、最大值、最小值
  private static List<String> supportedLambda = asList("COUNT", "SUM", "AVG", "DISTINCT", "MAX", "MIN");
  // 脱敏, 映射保留字或内部值
  private static HashMap<String, String> masks = new HashMap<String, String>() {
    {
      this.put("session", "cession");
    }
  };

  // 标识查询表, 从请求字段中提炼, 用于生成 SQL
  // 只需提炼 lambdaClause && whereClause 中的字段即可
  // 这个设计确实很烂, 明摆的副作用, 小心坑
  private final HashMap<String, HashMap<String, String>> tables = new HashMap();
  private final HashMap sqlParams = new HashMap();
  private final List<String> selectColumns = new ArrayList();
  private final List<String> rawGroupList = new ArrayList();
  private final List<String> unionWhere = new ArrayList();
  private List<JSONObject> rawHaving;


  public HttpServletRequest getRequest() {
    return request;
  }

  @Autowired
  protected HttpServletRequest request;

  @Autowired
  protected QueryImpl doQuery;

  protected BaseHandler() {
//    ParameterizedType p = (ParameterizedType) this.getClass().getGenericSuperclass();
//    Type[] types = p.getActualTypeArguments();
//    String[] splits = types[0].getTypeName().split("\\.");
//    // 约定大于配置
//    this.actualTypeName = English.plural(splits[splits.length - 1]).toLowerCase();

  }

  // table? family? qualify? 条件?
  // algo: 0 属性 1 指标
  // select=cessions.ua.gbrowser
  // &algo=1&type=number
  // &lambda=count&lambda=sum (newUsers)&calc=distinct&selector=first
  // &key=jsonkey
  // &where={"type":"number","events.name":"click","cond":"EQUAL","match":"AND","cascades":[{"ua.gbrowser":"chrome","cond":"CONTAINS","match":"AND"}]}
  // &where={"cessions.ua.gbrowser":"mozilla","cond":"CONTAINS","match":"OR"}
  // &groupby={"by":"sessions.geo.ip","type":"number"}
  // &having={"sessions.geo.ip":"127.0.0.1","cond":"EQUAL","match":"OR",type:"number"}
  // &orderby={"by":"sessions.ua.screen","desc":false,"type":"number"}
  // &startat=20170711010000&endat=20170811010000&utime=4,1&limit=1000&offset=100
  // limit从用户使用的角度看，没有价值，因为用户并不知道应该limit多少，offset面临一样的道理
  // 任何时候, 入参都需要一一过滤, 主要是防止 SQL 注入
  // 字段格式: TableName.FamilyName.QualifyName

  // 将复杂的查询拆分成最小查询单位，将计算压力转化为系统压力
  protected List<HashMap> handle(HttpServletRequest request, HttpServletResponse response,
                           String select,
                           List<JSONObject> where, List<JSONObject> groupBy,
                           List<JSONObject> having, List<JSONObject> orderBy) throws Exception {

    HashMap<String, String> clause = getClause(select, where, groupBy, having, orderBy);

    return doQuery.queryBySql(getSql(clause));
  }


  protected HashMap<String, String> getClauseWithUnionWhere(String select, List<JSONObject> where, List<JSONObject> groupBy,
                                              List<JSONObject> having, List<JSONObject> orderBy, List<JSONArray> unionWhereList) throws Exception {
    HashMap<String, String> rtn = getClause(select, where, groupBy, having, orderBy);

    for (JSONArray array : unionWhereList) {
      List<JSONObject> list = new ArrayList();
      for (int i = 0; i < array.length(); i++) {
        list.add(array.getJSONObject(i));
      }
      unionWhere.add(getWhere(list));
    }
    return rtn;
  }

  public List<String> getUnionWhere() {
    return unionWhere;
  }


  protected HashMap<String, String> getClause(String select, List<JSONObject> where, List<JSONObject> groupBy,
                                              List<JSONObject> having, List<JSONObject> orderBy) throws Exception {
    // 重置
    tables.clear();
    sqlParams.clear();
    selectColumns.clear();
    rawGroupList.clear();
    unionWhere.clear();

    String selectClause = select;
    String lambdaClause = "";
    lambdaClause = getLambda(select);

    ArrayList<JSONObject> whereList = new ArrayList();
    for (int i = 0; i < where.size(); i += 1) {
      whereList.add(where.get(i));
    }
    String whereClause = getWhere(whereList);
    String groupByClause = getGroupBy(groupBy);
    String havingClause = getHaving(having);
    String orderByClause = getOrderBy(orderBy);

    setSqlParams(selectClause, lambdaClause, whereClause, groupByClause, havingClause, orderByClause);
    return sqlParams;

  }

  public HashMap<String, HashMap<String, String>> getTables() {
    return tables;
  }

  public void setTables(HashMap<String, HashMap<String, String>> tables) {
    tables = tables;
  }

  public String getJoin() throws NoSuchFieldException, ClassNotFoundException {
    String tableParam = "";
    // 读取JOIN规则
    String[] schemas = tables.keySet().toArray(new String[0]);
    for (int i = 0, len = schemas.length; i < len; i++) {
      String primaryTable = schemas[i];
      Class clazz = classForTableName(primaryTable);
      for (int j = i + 1; j < len; j++) {
        String joinedTable = schemas[j];
        String entity = getTableRef(joinedTable).get(1).toLowerCase();
        Field field;

        // 主表与从表之间的关系只能是1对多或者1对1
        try {
          // 1对多
          field = clazz.getField(entity);
        } catch (Exception e0) {
          // 1对1
          try {
            field = clazz.getField(Inflector.getInstance().singularize(entity));
          } catch (Exception e1) {
            continue;
          }

        } finally {
          // Do Nothing
        }

        String onStr = "";
        // 约定: 用于join的字段不带列簇 (即不违背Phoenix字段最大支持3段)
        for (PrimaryKeyJoinColumn primaryKeyJoinColumn : field.getAnnotation(PrimaryKeyJoinColumns.class).value()) {
          onStr += (onStr.isEmpty() ? "" : " AND ") + primaryTable + "." + primaryKeyJoinColumn.name() +
            "=" + joinedTable + "." + primaryKeyJoinColumn.referencedColumnName();
        }

        tableParam += String.format("%s LEFT JOIN %s ON %s",
          tableParam.isEmpty() ? declareDynamicColumns(primaryTable) : "",
          declareDynamicColumns(joinedTable),
          onStr);

      }
      if (tableParam.isEmpty()) {
        tableParam = declareDynamicColumns(primaryTable);
      }
    }
    return tableParam;
  }



  // 生成单条 SELECT 语句
  private void setSqlParams(String selectClause, String lambdaClause,
                                               String whereClause, String groupByClause, String havingClause,
                                               String orderByClause) {
    String limit = request.getParameter("limit");
    String offset = request.getParameter("offset");
    if (limit == null) limit = "";
    if (offset == null) offset = "";

    sqlParams.put("select", selectClause);
    sqlParams.put("lambda", lambdaClause);
    sqlParams.put("where", whereClause);
    sqlParams.put("groupBy", groupByClause);
    sqlParams.put("having", havingClause);
    sqlParams.put("orderBy", orderByClause);
    sqlParams.put("limit", limit);
    sqlParams.put("offset", offset);
  }

  public HashMap<String, String> getSqlParams() {
    return sqlParams;
  }

  // 生成最终 SQL
  // 同意对比只发生在同等条件下才有意义
  // 此数据结构是能够支持在不同等条件下的对比 (事实上, 应该没有运用场景)
  // SqlParams: index, where, groupBy, having, orderBy, limit, offset
  private String getSql(HashMap<String, String> sqlParams) throws NoSuchFieldException, ClassNotFoundException {
    String lambda = sqlParams.getOrDefault("lambda", "");
    String where = sqlParams.getOrDefault("where", "");
    String groupBy = sqlParams.getOrDefault("groupBy", "");
    String having = sqlParams.getOrDefault("having", "");
    String orderBy = sqlParams.getOrDefault("orderBy", "");
    String limit = sqlParams.getOrDefault("limit", "");
    String offset = sqlParams.getOrDefault("offset", "");

    String[] groupByList = groupBy.isEmpty() ? null : groupBy.split(",");
    String whereFromGroupBy = groupByList != null ? (StringUtils.join(groupByList, " IS NOT NULL AND ") + " IS NOT NULL") : "";

    // 在某一时间范围内，按单位时间查看
    // K 线：分、多日、日、周、月、季、年
    // 单位时间：0: Milliseconds, 1: Seconds, 2: Minutes, 3: hour, 4: date, 5: week, 6: month, 7: year
    String utime = request.getParameter("utime");
    if (!StringUtils.isEmpty(utime)) {

      String timeGroup = String.format("%s.epoch", actualTypeName);
      timeGroup = getPhoenixMeta(timeGroup);

      selectColumns.add(String.format("%s(%s, %s) AS DATE", "UNIT_TIME", timeGroup, utime));
      groupBy += groupBy.isEmpty() ? "DATE" : ",DATE";
    }
    selectColumns.add(lambda);

    // 组装 sql
    // 举个例子，events 对应 LEFT JOIN，cessions 则对应 RIGHT JOIN
    String[] table = {};
    String sql = "SELECT " + String.join(",", selectColumns) + " FROM " + getJoin();
    if (!where.isEmpty()) sql += " WHERE " + where;
    if (!whereFromGroupBy.isEmpty()) sql += (where.isEmpty() ? " WHERE " : " AND ") + whereFromGroupBy;


    // Having 子句永远是为 GroupBy 子句服务
    if (!groupBy.isEmpty()) {
      sql += " GROUP BY " + groupBy;
      if (!having.isEmpty()) sql += " HAVING " + having;
    }
    if (!orderBy.isEmpty()) sql += " ORDER BY " + orderBy;
    if (!limit.isEmpty()) sql += " LIMIT " + limit;
    if (!offset.isEmpty()) sql += " OFFSET " + offset;
    return sql;
  }

  public String getLambda(String select) throws Exception {
    return getLambda(select, null);
  }

  public String getLambda(String select, com.alibaba.fastjson.JSONObject json) throws Exception {
    String algo, calc, jsonKey, type;
    String[] lambdaList = new String[]{};
    if (json != null) {
      algo = json.getString("algo");
      calc = json.getString("calc");
      jsonKey = json.getString("jsonkey");
      type = json.getString("type");
      lambdaList = json.getJSONArray("lambda").toArray(lambdaList);
    } else {
      algo = request.getParameter("algo");
      calc = request.getParameter("calc");
      jsonKey = request.getParameter("jsonkey");
      type = request.getParameter("type");

      if (request.getParameterValues("lambda") != null) lambdaList = request.getParameterValues("lambda");
    }

    boolean isAlgo = algo.equals("1");
    if (!isAlgo) {
      ArrayList<String> expr = new ArrayList();
      // 复合属性在此处定制
      boolean calcable = !StringUtils.isEmpty(calc);

      boolean hasJsonKey = !StringUtils.isEmpty(jsonKey);
      // 总是要注册字段信息
      // JSON字段总是存储为String
      select = getPhoenixMeta(select, hasJsonKey ? "String" : type);

      int i = 0;

      do {
        String lambda = lambdaList[i];

        if (!supportedLambda.contains(lambda.toUpperCase())) {
          // TODO: 此处若支持多个LAMBDA, 别名VAL仍需对应
          // 正态分布
          expr.add("PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY " + select + " DESC) AS VAL");
//            throw new Exception("Illegal parameter lambda " + lambda);
        } else {
          expr.add(lambda + "(" +
            (calcable ? (calc + " ") : "") + getPhoenixMetaOfJson(select, type, jsonKey, lambda) +
          ") AS VAL");
        }

      } while (++i < lambdaList.length);

      return String.join(",", expr);
    } else {
      // 1次只支持1个指标
      return request.getParameterValues("lambda")[0];
    }
  }



  // where={"ua.browser":"mozilla","cond":"CONTAINS","match":"OR"}&where=
  private String getWhere(List<JSONObject> where) throws JSONException {
    String whereCond = getCondition(where);
    String whereClause = !whereCond.isEmpty() ? whereCond : "";
    return whereClause;
  }

  // 分组
  // 暂时不支持聚合函数
  // groupby=sess.ip&groupby=prop.epoch
  private String getGroupBy(List<JSONObject> groupByList) {
    List<String> groupBy = by(groupByList, "GROUP");

    String timeGroup = String.format("%s.EPOCH", actualTypeName);
    // 拒绝前端恶意按时间最小单位（秒）分组
    int i = groupBy.indexOf(timeGroup);
    if (-1 != i) groupBy.remove(i);
    String groupByClause = !groupBy.isEmpty() ? String.join(",", groupBy) : "";

    // 默认查询组子句中的字段
    selectColumns.addAll(groupBy);
    return groupByClause;
  }

  // having={"ua.browser":"mozilla","cond":"GT","cond":"OR"}&having=
  private String getHaving(List<JSONObject> having) throws JSONException {
    String havingClause = "";
    rawHaving = having;
    String havingCond = getCondition(having);
    if (!havingCond.isEmpty()) havingClause = havingCond;
    return havingClause;
  }

  // orderby={"by":"ua.browser","desc":"true"}&orderby={"by":"ua.screen","desc":"false"}
  private String getOrderBy(List<JSONObject> orderByList) throws JSONException {
    return String.join(",", by(orderByList, "ORDER"));
  }

  private List<String> by(List<JSONObject> object, String clauseType) {
    List<String> list = new ArrayList();
    if (object == null) return list;
    if (!object.isEmpty()) {

      for (JSONObject map : object) {
        String mapStr = "";
        // `by` 键是必要条件
        String by = map.optString("by");
        String type = map.optString("type");
        if (!by.isEmpty()) {
          // 记录未经修改的group全名
          rawGroupList.add(by.toUpperCase());

          by = getPhoenixMeta(by, type);
          mapStr += by;

          if (clauseType.toUpperCase().equals("ORDER"))
            // 默认降序
            mapStr += map.optBoolean("desc", true) ? " DESC" : " ASC";
          list.add(mapStr);

        }
      }
    }
    return list;
  }


  public String getCondition(List<JSONObject> ast, boolean alwaysStripTablePrefix) throws JSONException {
    if (ast == null) return "";
    List<String> list = new ArrayList();

    if (!ast.isEmpty()) {
      Iterator<JSONObject> iterator = ast.iterator();
      while (iterator.hasNext()) {
        JSONObject map = iterator.next();
        String match = map.optString("match");
        map.remove("match");
        String cond = map.optString("cond");
        if (cond.isEmpty()) continue;
        cond = cond.toUpperCase();
        map.remove("cond");
        JSONArray range = map.optJSONArray("range");
        map.remove("range");
        String type = map.optString("type");
        map.remove("type");
        String jsonKey = map.optString("jsonKey");
        map.remove("jsonKey");
        String lambda = map.optString("lambda");
        map.remove("lambda");

        JSONArray optCascades = map.optJSONArray("cascades");
        List<JSONObject> cascades = new ArrayList<>();
        int size = (optCascades == null) ? 0 : optCascades.length();
        while (size-- > 0) cascades.add(optCascades.getJSONObject(size));
        boolean hasCascades = !cascades.isEmpty();
        if (hasCascades) {
          list.add("("); // 左边界
          map.remove("cascades");
        }

        int count = 0;
        for (Iterator it = map.keys(); it.hasNext(); ) {
          // 正常情况下 map 中只有一对键值对，水平扩展
          if (count++ > 0) list.add("AND");

          // 此处的 key 都会带上表名, 列簇
          // family-key, 无需table前缀
          String key = (String) it.next();
          String val = map.optString(key);
          // 注意若水平扩展, 这里假设字段类型一致

          if (!alwaysStripTablePrefix) {
            // getPhoenixMeta修改key, getPhoenixMetaOfJson不修改
            key = getPhoenixMeta(key, jsonKey.isEmpty() ? type : "String");
            key = getPhoenixMetaOfJson(key, type, jsonKey, lambda);
          } else {
            key = getPhoenixMetaOfJson(stripTablePrefix(key, true), type, jsonKey, lambda);
          }

          if (cond.equals("LIKE")) {
            list.add(String.format("%s LIKE '%s'", key, val));
          } else if (cond.equals("NOT LIKE")) {
            list.add(String.format("%s NOT LIKE '%s'", key, val));
          } else if (cond.equals("EQ")) {
            list.add(String.format("%s='%s'", key, val));
          } else if (cond.equals("NEQ")) {
            list.add(String.format("%s!='%s'", key, val));
          } else if (cond.equals("EGT")) {
            list.add(String.format("%s>=%s", key, val));
          } else if (cond.equals("LGT")) {
            list.add(String.format("%s<=%s", key, val));
          } else if (cond.equals("IS NULL")) {
            list.add(String.format("%s IS NULL", key));
          } else if (cond.equals("IS NOT NULL")) {
            list.add(String.format("%s IS NOT NULL", key));
          } else if (cond.equals("CONTAINS")) {
            list.add(String.format("'%s' = ANY(%s)", val, key));
          } else if (cond.equals("RANGE")) {
            if (2 == range.length())
              list.add(String.format("%s BETWEEN %d AND %d", key, range.getDouble(0), range.getDouble(1)));
          }
        }


        if (hasCascades) {
          // 级联条件和宿主条件是AND关系
          list.add("AND");
          list.add(getCondition(cascades, alwaysStripTablePrefix));
          // 右边界
          list.add(")");
        }

        // 忽略最后一个条件的逻辑操作符
        if (!iterator.hasNext() || match.isEmpty()) break;
        list.add(match.toUpperCase());

      }
    }

    return list.isEmpty() ? "" : String.join(" ", list);
  }

  private String getCondition(List<JSONObject> ast) throws JSONException {
    return getCondition(ast, false);
  }

  // 提取表, 列簇
  private String getPhoenixMeta(String meta, String type) {
    return addPhoenixMeta(meta, convertToDynamicType(type));
  }

  // 转化为Phoenix支持的TYPE
  private String convertToDynamicType(String type) {
    String dynamicType = "VARCHAR";
    if (StringUtils.isEmpty(type)) type = "STRING";
    type = type.toUpperCase();
    if (type.equals("NUMBER")) dynamicType = "DECIMAL";
    if (type.equals("LIST[NUMBER]")) dynamicType = "DECIMAL[]";
    if (type.equals("LIST[STRING]")) dynamicType = "VARCHAR[]";
    if (type.equals("LIST[BOOLEAN]")) dynamicType = "BOOLEAN[]";
    return dynamicType;
  }

  private String getPhoenixMeta(String meta) {
    String dynamicType = "VARCHAR";
    return addPhoenixMeta(meta, dynamicType);
  }

  private String addPhoenixMeta(String meta, String dynamicType) {
    // 注意区别JS中split的limit
    String[] splits = meta.split(COLUMN_SPLITOR, 2);
    if (1 == splits.length) try {
      throw new Exception("Illegal parameter value " + splits[0]);
    } catch (Exception e) {
      e.printStackTrace();
    }

    String table = TABLE_PREFIX + splits[0];
    String column = splits[1];
    addDynamicColumn(table, column, dynamicType);
    // Phoenix BUG: 带有family的字段不需要带schema前缀（包括表名, 除非使用别名）
    if (column.indexOf(".") != -1) return TABLE_PREFIX + column;
    return TABLE_PREFIX + meta.toUpperCase();
  }

  private Class classForTableName(String tableName) throws ClassNotFoundException {
    ArrayList<String> segments = getTableRef(tableName);

    Class<?> clazz = Class.forName(DOMAIN_PACKAGE + "." +
//      (segments.get(0).isEmpty() ? "" : (segments.get(0).toLowerCase() + ".")) +
      StringUtils.capitalize(
        Inflector.getInstance().singularize(
          segments.get(1).toLowerCase())));

    return clazz;
  }

  // 属于表基础字段亦或动态字段
  private boolean isDynamicColumn(String table, String column) {
    if (column.indexOf(".") != -1) return true;
    Class clazz = null;
    try {
      clazz = classForTableName(table);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      if (field.getName().equals(column)) return true;
    }
    return false;
  }

  // 针对JSONArray && JSONObject的字段定义
  private String getPhoenixMetaOfJson(String select, String type, String jsonKey, String lambda) {
    boolean hasJsonKey = !StringUtils.isEmpty(jsonKey);
    boolean isArray = false;
    if (!StringUtils.isEmpty(type)) isArray = (type.toUpperCase().indexOf("LIST") == 0);
    boolean withAgg = isArray && !StringUtils.isEmpty(lambda);
    // JSON元素的dynamicType
    String dynamicType = convertToDynamicType(type);
    String singularType = dynamicType.substring(0, (dynamicType.length() - 2));

    if (hasJsonKey) {
      if (isArray) {
        if (StringUtils.isEmpty(lambda)) select = String.format("JSON_ELEM_ARRAY(%s, '%s', '%s', 'VALUES')", select, jsonKey, singularType);
      } else {
        select = String.format("JSON_ELEM(%s, '%s', '%s')", select, jsonKey, dynamicType);
      }
    }

    return (withAgg
      ? (hasJsonKey
      ? String.format("JSON_ELEM_AGG(%s, '%s', '%s', '%s')", select, jsonKey, singularType, lambda)
      : String.format("ARRAY_AGG(%s, '%s')", select, lambda)
    )
      : select
    );
  }

  // Scalar里如何优雅使用匿名函数
  private String declareDynamicColumns(String table) {
    HashMap<String, String> columns = tables.get(table);
    ArrayList<String> joins = new ArrayList();
    for (Map.Entry<String, String> column : columns.entrySet()) {
      String family = column.getKey();
      String type = column.getValue();
      joins.add(family + " " + type);
    }
    return table + (joins.isEmpty() ? "" : "(" + String.join(",", joins) + ")");
  }

  // 输出所有Dynamic Columns
  // 注意, 包含表基础字段
  public ArrayList<String> getAllDynamicColumns(boolean withTablePrefix, boolean withColumnType) {
    ArrayList<String> dynamicColumns = new ArrayList();
    for (Map.Entry<String, HashMap<String, String>> table : tables.entrySet()) {
      HashMap<String, String> columns = table.getValue();
      for (Map.Entry<String, String> column : columns.entrySet()) {
        // phoenix奇葩问题：带有family的字段不需要带schema前缀（包括表名, 除非使用别名）
        boolean _withTablePrefix = withTablePrefix && (column.getKey().indexOf(".") == -1);
        dynamicColumns.add(
          (_withTablePrefix ? table.getKey() + "." : "") +
            column.getKey()
            + (withColumnType ? " " + column.getValue() : ""));
      }
    }
    return dynamicColumns;
  }


  public void addDynamicColumn(String table, String column, String type) {
    if (StringUtils.isEmpty(type)) type = "VARCHAR";
    table = table.toUpperCase();
    column = column.toUpperCase();
    type = type.toUpperCase();

    HashMap<String, String> columns = new HashMap();
    if (!tables.containsKey(table)) tables.put(table, columns);
    columns = tables.get(table);
    // 不带列簇的字段都是表的基础字段, 在创建表时自定义类型, 带列簇的字段类型都为 VARCHAR
    if (!columns.containsKey(column) && isDynamicColumn(table, column)) columns.put(column, type);
  }

  // 根据全key获取一个dynamic column的信息
  public String[] getDynamicColumn(String key) {
    String[] splits = key.split(COLUMN_SPLITOR, 2);
    String table = splits[0];
    String column = splits[1];
    if (!tables.containsKey(table)) return new String[0];
    String dynamicType = tables.get(table).get(column);
    return new String[] {table, column, dynamicType};
  }

  public List<String> getRawGroupList() {
    return rawGroupList;
  }

  public List<JSONObject> getRawHaving() {
    return rawHaving;
  }

  public String addTimeRange(Boolean withAndPrefix) {
    String rtr = "";
    // 不同端的时间同步依靠时间戳传递即可
    String startAt = request.getParameter("startat");
    String endAt = request.getParameter("endat");

/*    if (StringUtils.isBlank(endAt)) {
      Calendar calendar = Calendar.getInstance();
      Date date = new Date(System.currentTimeMillis());
      calendar.setTime(date);
      // 103 年生命周期, 大于阿里102年
      calendar.add(Calendar.YEAR, +103);
      endStamp = new Timestamp(calendar.getTimeInMillis());
    }*/

    List<String> timeRange = new ArrayList();

    if (!StringUtils.isBlank(startAt)) {
      Timestamp startStamp = new Timestamp(Long.valueOf(startAt));
      timeRange.add(String.format("%s.epoch>=TO_TIMESTAMP('%s')", TABLE_PREFIX + actualTypeName, startStamp));
    }
    if (!StringUtils.isBlank(endAt)) {
      Timestamp endStamp = new Timestamp(Long.valueOf(endAt));
      timeRange.add(String.format("%s.epoch<=TO_TIMESTAMP('%s')", TABLE_PREFIX + actualTypeName, endStamp));
    }
    if (timeRange.size() == 0) return "";
    rtr = timeRange.size() == 1 ? timeRange.get(0) : String.join(" AND ", timeRange);
    if (withAndPrefix && !rtr.isEmpty()) rtr = " AND " + rtr;
    return rtr;
  }

  private ArrayList<String> getTableRef(String tableName) {
    ArrayList<String> segments = new ArrayList(Arrays.asList(tableName.split("\\.")));
    if (1 == segments.size()) {
      segments.add(0, "");
    }
    return segments;
  }

  // 剥除COLUMN中的表前缀
  // 注意: always为true时, 入参一定要是没被动过的rawKey
  public String stripTablePrefix(String key, Boolean always) {
    int index = key.indexOf(".");
    if (-1 == index) {
      try {
        throw new Exception("Illegal column " + key);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    String[] splits = key.split(COLUMN_SPLITOR);
    if (always && splits.length == 2) key = splits[1];
    else if (splits.length == 3) {
      key =  splits[1] + "." + splits[2];
    }
    return key;
  }

  public String stripTablePrefix(String prefix, List<String> columns) {
    if (columns.isEmpty()) return "";
    List<String> list = new ArrayList();
    for (int i = 0; i < columns.size(); i++) {
      String column = stripTablePrefix(columns.get(i), true);
      list.add(i, prefix + column);
    }
    return String.join(",", list);
  }

  protected Object compute(HttpServletRequest request, Class clazz ,
                    String select,
                    List<JSONObject> where,
                    List<JSONArray> unionWhere,
                    List<JSONObject> groupBy,
                    List<JSONObject> having,
                    List<JSONObject> orderBy,
                    Boolean pageable,
                    Pageable page) throws Exception {
    Object rs = null;
    HashMap<String, String> sqlParams = getClauseWithUnionWhere(select, where, groupBy, having, orderBy, unionWhere);

    String lambdaParam = sqlParams.getOrDefault("lambda", "");

    // Java Dynamic Dispatch
    Method method = ReflectionUtils.findMethod(clazz, lambdaParam, new Class[]{BaseHandler.class, String.class, Pageable.class});
    if (null != method) {
      rs = ReflectionUtils.invokeMethod(method, AnalyticsApp.getBean(StringUtils.uncapitalize(clazz.getSimpleName())), this, null, BooleanUtils.isTrue(pageable) ? page : null);
    }
    return rs;
  }



}
