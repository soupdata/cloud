package com.godalgo.inspirer.cloud.analytics.handler;

/**
 * Created by 杭盖 on 2018/06/19.
 *
 * ElasticSearch似乎更适用于无规律，无序的“片段”搜索，需要基于片段存档
 * 若片段之间无需发生复杂的业务关系，则比较适用
 *
 * 若涉及到SQL结构发生变化, 优先建议使用新的API
 */

import com.godalgo.inspirer.cloud.analytics.annoation.*;
import com.godalgo.inspirer.cloud.analytics.domain.EventCession;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.Query;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

@RestController
@RequestMapping("/do/search")
public class SearchHandler extends BaseHandler<EventCession> {
  @Autowired
  private QueryImpl doQuery;

/*  @RequestMapping(value = "/", method = RequestMethod.GET)
  public ModelAndView index(HttpServletResponse response,
                            HttpServletRequest request) throws MalformedURLException {
    return new ModelAndView("index");

  }*/

  // 搜索
  // 原则：可用性定位大于技术定位
  // 三个级别: 1. NODETEXT 2. SELECTOR 3. XPATH
  // 从可用性角度来看, 用户搜索的是关键字或显式事件名（有些事件并没有NODETEXT）, 而非SELECTOR和XPATH
  // 这样来定位一个“唯一”元素（下面是一个形象的下拉列表项）：
  // 同一个元素定义: PAGEURL -> EVENTS.NAME -> NODETEXT（可用性） -> SELECTOR（技术, EVENTS.NAME为约束条件）-> XPATH（去掉序号，展示时分组）
  // 同一个页面定义: 去除参数的页面地址
  // 现实场景中往往查找的是一个跨页面的元素
  // 其实根本无需追溯到XPATH这一层, XPATH只有在name或者ID都缺失的情况下才有意义, 单独使用场景是不合理的
  @RequestMapping(value = "/searchByKey", method = RequestMethod.GET)
  @Query(value = "SELECT L.NAME AS NAME,ELEMENT.GA_NODETEXT AS TEXT FROM " +
    "EVENTS L(ELEMENT.GA_NODETEXT VARCHAR) " +
    "WHERE UPPER(ELEMENT.GA_NODETEXT) LIKE '%#{param.get('key').toUpperCase()}%' " +
    "GROUP BY NAME,TEXT", nativeQuery = true)
  @GenericDomain(enable = true)
  @AddArg(key = "groupKey", value = "NAME")
  public @ResponseBody
  Object searchByKey(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }

  @RequestMapping(value = "/searchByKeyAtPage", method = RequestMethod.GET)
  @Query(value = "SELECT MAX(PAGE.GA_TITLE) AS TITLE,PAGE.GA_PAGEURL AS URL,L.NAME AS NAME,ELEMENT.GA_NODETEXT AS TEXT FROM " +
    "EVENTS L(ELEMENT.GA_NODETEXT VARCHAR) LEFT JOIN CESSIONS R(PAGE.GA_TITLE VARCHAR,PAGE.GA_PAGEURL VARCHAR) ON L.DOMAIN=R.DOMAIN AND L.SID=R.SID AND L.MSEC=R.EPOCH " +
    "WHERE UPPER(ELEMENT.GA_NODETEXT) LIKE '%#{param.get('key').toUpperCase()}%' " +
    "GROUP BY URL,NAME,TEXT ORDER BY MAX(R.EPOCH) DESC", nativeQuery = true)
  @GenericDomain(enable = true)
  @AddArg(key = "groupKey", value = "URL,NAME")
  public @ResponseBody
  Object searchByKeyAtPage(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }


  @RequestMapping(value = "/searchByKeyWithSelector", method = RequestMethod.GET)
  @Query(value = "SELECT NAME,ELEMENT.GA_NODETEXT AS TEXT,ELEMENT.GA_SELECTOR AS SELECTOR FROM " +
    "EVENTS L(ELEMENT.GA_NODETEXT VARCHAR,ELEMENT.GA_SELECTOR VARCHAR) " +
    "WHERE UPPER(ELEMENT.GA_NODETEXT) LIKE '%#{param.get('key').toUpperCase()}%' " +
    "GROUP BY NAME,SELECTOR,TEXT", nativeQuery = true)
  @GenericDomain(enable = true)
  @AddArg(key = "groupKey", value = "NAME,SELECTOR")
  public @ResponseBody
  Object searchByKeyWithSelector(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }

  @RequestMapping(value = "/searchByKeyWithSelectorAtPage", method = RequestMethod.GET)
  @Query(value = "SELECT MAX(PAGE.GA_TITLE) AS TITLE,PAGE.GA_PAGEURL AS URL,L.NAME AS NAME,ELEMENT.GA_NODETEXT AS TEXT,ELEMENT.GA_SELECTOR AS SELECTOR FROM " +
    "EVENTS L(ELEMENT.GA_NODETEXT VARCHAR,ELEMENT.GA_SELECTOR VARCHAR) LEFT JOIN CESSIONS R(PAGE.GA_TITLE VARCHAR,PAGE.GA_PAGEURL VARCHAR) ON L.DOMAIN=R.DOMAIN AND L.SID=R.SID AND L.MSEC=R.EPOCH " +
    "WHERE UPPER(ELEMENT.GA_NODETEXT) LIKE '%#{param.get('key').toUpperCase()}%' " +
    "GROUP BY URL,NAME,SELECTOR,TEXT ORDER BY MAX(R.EPOCH) DESC", nativeQuery = true)
  @GenericDomain(enable = true)
  @AddArg(key = "groupKey", value = "URL,NAME,SELECTOR")
  public @ResponseBody
  Object searchByKeyWithSelectorAtPage(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }

  // XPATH跨页面没有意义
  @RequestMapping(value = "/searchByKeyWithXpathAtPage", method = RequestMethod.GET)
  @Query(value = "SELECT MAX(PAGE.GA_TITLE) AS TITLE,PAGE.GA_PAGEURL AS URL,L.NAME AS NAME,ELEMENT.GA_NODETEXT AS TEXT,ELEMENT.GA_XPATH AS XPATH FROM " +
    "EVENTS L(ELEMENT.GA_NODETEXT VARCHAR,ELEMENT.GA_XPATH VARCHAR) LEFT JOIN CESSIONS R(PAGE.GA_TITLE VARCHAR,PAGE.GA_PAGEURL VARCHAR) ON L.DOMAIN=R.DOMAIN AND L.SID=R.SID AND L.MSEC=R.EPOCH " +
    "WHERE UPPER(ELEMENT.GA_NODETEXT) LIKE '%#{param.get('key').toUpperCase()}%' " +
    "GROUP BY URL,NAME,XPATH,TEXT ORDER BY MAX(R.EPOCH) DESC", nativeQuery = true)
  @GenericDomain(enable = true)
  @AddArg(key = "groupKey", value = "URL,NAME,XPATH")
  public @ResponseBody
  Object searchByKeyWithXpathAtPage(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }


  @RequestMapping(value = "/searchByName", method = RequestMethod.GET)
  @Query(value = "SELECT NAME AS TEXT FROM EVENTS " +
    "WHERE UPPER(NAME) LIKE '%#{param.get('key').toUpperCase()}%' " +
    "GROUP BY TEXT", nativeQuery = true)
  @GenericDomain(enable = true)
  @AddArg(key = "groupKey", value = "TEXT")
  public @ResponseBody
  Object searchByName(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }

  @RequestMapping(value = "/searchByNameAtPage", method = RequestMethod.GET)
  @Query(value = "SELECT MAX(PAGE.GA_TITLE) AS TITLE,PAGE.GA_PAGEURL AS URL,L.NAME AS TEXT FROM " +
    "EVENTS L LEFT JOIN CESSIONS R(PAGE.GA_TITLE VARCHAR,PAGE.GA_PAGEURL VARCHAR) ON L.DOMAIN=R.DOMAIN AND L.SID=R.SID AND L.MSEC=R.EPOCH " +
    "WHERE UPPER(L.NAME) LIKE '%#{param.get('key').toUpperCase()}%' " +
    "GROUP BY URL,TEXT ORDER BY MAX(R.EPOCH) DESC", nativeQuery = true)
  @GenericDomain(enable = true)
  @AddArg(key = "groupKey", value = "URL")
  public @ResponseBody
  Object searchByNameAtPage(HttpServletRequest request, String nativeSql) {
    List<HashMap> rs = doQuery.queryBySql(nativeSql);
    return rs;
  }
}
