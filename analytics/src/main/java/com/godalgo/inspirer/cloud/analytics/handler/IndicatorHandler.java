package com.godalgo.inspirer.cloud.analytics.handler;


import com.godalgo.inspirer.cloud.analytics.domain.EventCession;
import com.godalgo.inspirer.cloud.analytics.repository.IndicatorRepository;
import com.godalgo.inspirer.cloud.analytics.repository.StepRepository;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;

/**
 * Created by 杭盖 on 2018/9/28.
 */
@RestController
@RequestMapping("/do/indicator/search")
public class IndicatorHandler extends BaseHandler<EventCession> {
  @Autowired
  private QueryImpl doQuery;

  // 获取指标列表
  @RequestMapping(value = "/index", method = RequestMethod.GET)
  @Query(value = "SELECT * FROM INDICATORS WHERE #{domainCondition}", nativeQuery = true)
  public @ResponseBody
  List<HashMap> getIndicators(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }

  // 新建或更新指标
  @RequestMapping(value = "/upsert", method = RequestMethod.GET)
  @Query(value = "UPSERT INTO INDICATORS(DOMAIN,NAME,JSON) VALUES('#{domain}','#{param.get('name')}','#{param.get('json')}')", nativeQuery = true)
  @Transactional
  public @ResponseBody
  int upsert(HttpServletRequest request, String nativeSql) {
    return doQuery.updateSql(nativeSql);
  }

  // 删除指标
  @RequestMapping(value = "/delete", method = RequestMethod.GET)
  @Query(value = "DELETE FROM INDICATORS WHERE #{domainCondition} AND NAME='#{param.get('name')}'", nativeQuery = true)
  @Transactional
  public @ResponseBody
  int delete(HttpServletRequest request, String nativeSql) {
    return doQuery.updateSql(nativeSql);
  }

  // 算法入口
  // -- event VARCHAR,
  // -- select VARCHAR(48),
  // -- lambda VARCHAR(24) DEFAULT 'SUM',
  // -- selector VARCHAR(24) DEFAULT '0',
  // -- prefix_event VARCHAR DEFAULT '0',
  // -- suffix_event VARCHAR DEFAULT '0',

  @RequestMapping(value = "/compute", method = RequestMethod.GET)
  public @ResponseBody
  Object compute(HttpServletRequest request, HttpServletResponse response,
                 @RequestParam(value = "select", required = false, defaultValue = "") String select,
                 @RequestParam(value = "where", required = false, defaultValue = "") List<JSONObject> where,
                 @RequestParam(value = "unionwhere", required = false, defaultValue = "") List<JSONArray> unionWhere,
                 @RequestParam(value = "groupby", required = false, defaultValue = "") List<JSONObject> groupBy,
                 @RequestParam(value = "having", required = false, defaultValue = "") List<JSONObject> having,
                 @RequestParam(value = "orderby", required = false, defaultValue = "") List<JSONObject> orderBy,
                 @RequestParam(value = "pageable", required = false) Boolean pageable,
                 Pageable page
  ) throws Exception {
    return super.compute(request, IndicatorRepository.class, select, where, unionWhere, groupBy, having, orderBy, pageable, page);
  }

}
