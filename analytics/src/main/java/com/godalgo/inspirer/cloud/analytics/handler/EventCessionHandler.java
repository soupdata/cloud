package com.godalgo.inspirer.cloud.analytics.handler;

/**
 * Created by 杭盖 on 2018/1/3.
 */

import com.godalgo.inspirer.cloud.analytics.domain.EventCession;
import com.godalgo.inspirer.cloud.analytics.repository.EventCessionRepository;
import com.godalgo.inspirer.cloud.analytics.repository.StepRepository;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryPage;

import org.apache.commons.lang3.BooleanUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.WebApplicationContext;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.lang.reflect.Method;
import java.util.*;
// 线程级别
@Scope("prototype")
@RestController
@RequestMapping("/do/event/search")
public class EventCessionHandler extends BaseHandler<EventCession> {
  @Autowired
  private QueryImpl doQuery;

  @Autowired
  // 如果与 @RepositoryRestResource 初始化的非同一个对象，则通过调用 zuul 服务来派发请求
  protected EventCessionRepository eventCessionRepository;

/*  @RequestMapping(value = "/", method = RequestMethod.GET)
  public ModelAndView index(HttpServletResponse response,
                            HttpServletRequest request) throws MalformedURLException {
    return new ModelAndView("index");

  }*/

  // RFC 7230 and RFC 3986, Malformed Characters: `{` `}`
  // Notice `matching editors or conversion strategy`
  // JSONObject vs. JsonObject

  @RequestMapping(value = "/", method = RequestMethod.GET/*, value = "/stores/{storeId}", produces = "application/hal+json"*/)
  public @ResponseBody
  List<HashMap> handleSearch(HttpServletRequest request, HttpServletResponse response,
                       @RequestParam(value = "select") String select,
                       @RequestParam(value = "where", required = false) List<JSONObject> where,
                       @RequestParam(value = "groupby", required = false) List<JSONObject> groupBy,
                       @RequestParam(value = "having", required = false) List<JSONObject> having,
                       @RequestParam(value = "orderby", required = false) List<JSONObject> orderBy
  ) throws Exception {
    if (groupBy == null || groupBy.isEmpty()) groupBy = new ArrayList();
    if (having == null || having.isEmpty()) having = new ArrayList();
    if (orderBy == null || orderBy.isEmpty()) orderBy = new ArrayList();
    return handle(request, response, select, where, groupBy, having, orderBy);
  }

  // 所有算法入口
  // unionwhere针对多步骤, 每一步骤之间的条件是UNION的关系, 专属于算法(用于装载基础表events_cessions)
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
    return super.compute(request, EventCessionRepository.class, select, where, unionWhere, groupBy, having, orderBy, pageable, page);
  }

  @RequestMapping(value = "/xx", method = RequestMethod.GET/*, value = "/stores/{storeId}", produces = "application/hal+json"*/)
  @Transactional
  public @ResponseBody
  Object xx(HttpServletRequest request, HttpServletResponse response
  ) throws Exception {

    Object rs = doQuery.updateSql("DELETE FROM VIEW_STEPS WHERE UUID='6bab7d60-a5ac-4ebf-ac75-b062dccbdab5'");
    return rs;
  }


}
