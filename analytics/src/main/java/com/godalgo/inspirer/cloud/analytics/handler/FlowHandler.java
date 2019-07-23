package com.godalgo.inspirer.cloud.analytics.handler;

/**
 * Created by 杭盖 on 2018/08/21.
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.godalgo.inspirer.cloud.analytics.AnalyticsApp;
import com.godalgo.inspirer.cloud.analytics.domain.EventCession;
import com.godalgo.inspirer.cloud.analytics.repository.EventCessionRepository;
import com.godalgo.inspirer.cloud.analytics.repository.StepRepository;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.apache.commons.lang3.BooleanUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.Method;
import java.util.*;
@Scope("prototype")
@RestController
@RequestMapping("/do/flow/search")
public class FlowHandler extends BaseHandler<EventCession> {
  @Autowired
  private QueryImpl doQuery;

  @Autowired
  protected StepRepository stepRepository;

  // 获取漏斗列表
  @RequestMapping(value = "/index", method = RequestMethod.GET)
  @Query(value = "SELECT * FROM STEPS WHERE #{domainCondition} AND TYPE=1", nativeQuery = true)
  public @ResponseBody
  List<HashMap> getFlows(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }

  // 新建或更新漏斗
  @RequestMapping(value = "/upsert", method = RequestMethod.GET)
  @Transactional
  public @ResponseBody
  int upsert(HttpServletRequest request,
             @RequestParam(value = "name", required = true) String name,
             @RequestParam(value = "steps", required = true) List<String> steps) {
    String domain = (String) request.getSession().getAttribute("domain");
    String jsonSteps = JSON.toJSONString(JSON.toJSON(steps), SerializerFeature.UseSingleQuotes);
    String nativeSql = String.format("UPSERT INTO STEPS(DOMAIN,TYPE,NAME,STEPS,EPOCH) VALUES('%s',1,'%s',ARRAY%s,NOW())", domain, name, jsonSteps);
    return doQuery.updateSql(nativeSql);
  }

  // 删除漏斗
  @RequestMapping(value = "/delete", method = RequestMethod.GET)
  @Query(value = "DELETE FROM STEPS WHERE #{domainCondition} AND NAME='#{param.get('name')}' AND TYPE=1", nativeQuery = true)
  @Transactional
  public @ResponseBody
  int delete(HttpServletRequest request, String nativeSql) {
    return doQuery.updateSql(nativeSql);
  }

  // 算法入口
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
    return super.compute(request, StepRepository.class, select, where, unionWhere, groupBy, having, orderBy, pageable, page);
  }
}
