/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.godalgo.inspirer.cloud.analytics.repository;


import com.godalgo.inspirer.cloud.analytics.annoation.*;

import com.godalgo.inspirer.cloud.analytics.handler.BaseHandler;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


/**
 * 指标体系
 *
 * Hibernate 不支持 Phoenix-Style SQL的某些数据类型, 比如 ARRAY, 会抛出org.hibernate.MappingException: No Dialect mapping for JDBC type: 2003
 * 因此不再继承JpaRepository
 * 保留@Query注解, 仅仅是为了高亮语法, 具体解析由Velocity负责, 而非SpEL语法
 *
 * Phoenix视图坑爹, http://phoenix.apache.org/views.html
 * A VIEW may be defined over only a single table through a simple SELECT * query.
 * You may not create a VIEW over multiple, joined tables nor over aggregations
 *
 * 测试结果显示Phoenix并不支持关联子查询
 *
 * Phoenix目前没有一种能够拿到Dynamic Columns的方法, 除非在SQL中显示声明,
 * 意味着中间表的解决方案行不通
 *
 * 保留字 START FIRST
 *
 * @author 杭盖
 */
//@RepositoryRestResource(collectionResourceRel = "event", path = "event")
@Repository
public class EventCessionRepository {
  @Autowired
  private QueryImpl doQuery;

/*  @RestResource(rel = "by-location")
  Page<Event> findByAddressLocationNear(Pageable pageable);

  Page<Event> withNameAndAddressNamedQuery(String name, String address, Pageable pageable);*/

  // 活跃用户数
  // 定义: 单位时间 (00:00-24:00) 之内, 访问网站的不重复用户数 (以浏览器 Cookie 为依据),
  // 时活, 日活, 周活, 半月活, 月活, 季活, 年活
  // 单位时间同一访客多次访问网站只被计算1次
  @I18N({
    @AddI18N(lang = "cn", value = "活跃用户数", code = "ActiveUser")
  })
  @Query(value =
    "SELECT COUNT(DISTINCT SID) AS VISITS,UNIT_TIME(EPOCH,#{utime}) AS UNIT #{group} " +
    "FROM EVENTS_CESSIONS GROUP BY UNIT #{group} #{having}",
    nativeQuery = true)
  @Transactional
  public Page<?> activeUsers(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // PV 页面浏览量
  // 定义: 网页浏览是指浏览器加载 (或重新加载) 网页的实例, 页面浏览量可以定义为网页浏览总次数的指标
  @I18N({
    @AddI18N(lang = "cn", value = "页面浏览量", code = "PV")
  })
  @AddDynamicColumns({
    @AddDynamicColumn(column = "CESSIONS.PAGE.PID", type = "VARCHAR")
  })
  @Query(value =
    "SELECT COUNT(DISTINCT PAGE.PID) AS VISITS,UNIT_TIME(EPOCH,#{utime}) AS UNIT #{group} " +
    "FROM EVENTS_CESSIONS GROUP BY UNIT #{group}",
    nativeQuery = true)
  @Transactional
  public Page<?> pageViews(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

///*  // 新增注册用户数
//  // 定义: 单位时间注册用户数, 前台通过触发注册成功事件
//  @View(value = "SELECT COUNT(DISTINCT )")
//  List<Integer> newRegisterUsers(@Param("utime") String utime);*/

  // 浏览率 browseRate

  // 新访客
  // 定义: 当日的独立访客中, 历史上首日访问网站的访客定义为新访客
  // Phoenix 非 where 子句支持 NOT IN 和 NOT EXISTS 有问题
  @I18N({
    @AddI18N(lang = "cn", value = "新访客", code = "NewVisitor")
  })
  @Query(value =
    "SELECT COUNT(SID) AS VISITS,UNIT_TIME(_UNIT,#{utime}) AS UNIT #{group} FROM (" +
      "SELECT SID,FIRST_VALUE(EPOCH) WITHIN GROUP (ORDER BY EPOCH ASC) AS _UNIT" +
      "FROM EVENTS_CESSIONS WHERE NAME=LOWER('LOAD') GROUP BY SID ORDER BY MIN(EPOCH) ASC" +
    ") " +
    "GROUP BY UNIT #{group}",
    nativeQuery = true)
  @Transactional
  @Backdate(enable = true)
  Page<?> newVisitors(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 回头客
  // 定义: 当日的独立访客中, 历史上第二次访问网站的访客定义为回头客
  // Phoenix针对关联子查询内部有重构
  @I18N({
    @AddI18N(lang = "cn", value = "回访客", code = "ReturnVisitor")
  })
  @Query(value =
    "SELECT COUNT(SID) AS VISITS,UNIT #{group} FROM (" +
      "SELECT SID,UNIT_TIME(EPOCH,#{utime}) AS UNIT FROM EVENTS_CESSIONS WHERE NAME=LOWER('LOAD') GROUP BY SID,UNIT" +
    ") " +
    "WHERE (SID, UNIT) NOT EXISTS (" +
      "SELECT SID,UNIT_TIME(FIRST_VALUE(EPOCH) WITHIN GROUP (ORDER BY EPOCH ASC),#{utime}) AS UNIT FROM EVENTS_CESSIONS " +
      "WHERE NAME=LOWER('LOAD') " +
      "GROUP BY SID ORDER BY MIN(EPOCH) ASC" +
    ") " +
    "GROUP BY UNIT #{group}",
    nativeQuery = true)
  @Transactional
  @Backdate(enable = true)
  Page<?> repeatVisitors(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 定义：当日的访客中，访客在所有访客中占的比例
  @I18N({
    @AddI18N(lang = "cn", value = "新老访客占比", code = "")
  })
  @Query(value =
    "SELECT NEW_VISITS,(TOTAL_VISITS-NEW_VISITS) AS REPEAT_VISITS,TOTAL_VISITS,UNIT FROM (" +
      "(SELECT COUNT(SID) AS TOTAL_VISITS,UNIT_TIME(EPOCH,#{utime}) AS UNIT #{group} " +
      "FROM EVENTS_CESSIONS WHERE NAME=LOWER('LOAD') GROUP BY SID,UNIT #{group}) AS TOTAL " +
      "LEFT JOIN " +
      "(SELECT COUNT(SID) AS NEW_VISITS,UNIT #{group} FROM" +
        "(SELECT SID,UNIT_TIME(FIRST_VALUE(EPOCH) WITHIN GROUP (ORDER BY EPOCH ASC),#{utime}) AS UNIT " +
        "FROM EVENTS_CESSIONS " +
        "WHERE NAME=LOWER('LOAD') " +
        "GROUP BY SID ORDER BY MIN(EPOCH) ASC) " +
      "GROUP BY UNIT #{group}) AS NEW " +
      "ON TOTAL.UNIT=NEW.UNIT" +
    ")",
    nativeQuery = true)
  @Transactional
  @Backdate(enable = true)
  Page<?> visitorRatio(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 访问次数: 衡量单位时间内完成某一指定操作的频次
  // 比如30分钟内完成下单动作的次数
  // 延展, 30分钟内完成下单动作[次数]最多的[用户]
  // 会话相对于用户而存在
  // 定义: 访客从进入网站到离开网站的一系列活动（或者包含某些特定的事件）记为一次访问, 也称会话 (Activity)
  // @param activity
  @I18N({
    @AddI18N(lang = "cn", value = "访问次数", code = "Visits")
  })
  @Query(value =
    "SELECT COUNT(SID) AS VISITS,UNIT_TIME(_UNIT,#{utime}) AS UNIT #{group} FROM (" +
      "SELECT UNIT_TIME(A.EPOCH,B.TYPE) AS _UNIT, sid FROM " +
      "EVENTS_CESSIONS AS A " +
      "JOIN " +
      "(SELECT EVENTS,TYPE FROM ACTIVITIES WHERE NAME=#{param.get('activity')}) AS B " +
      "ON A.NAME=ANY(B.EVENTS)" +
      "GROUP BY _UNIT, sid" +
    ") " +
    "GROUP BY UNIT #{group}", nativeQuery = true)
  Page<?> activities(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 平均触发数
  // 触发总次数/触发用户数
  @I18N({
    @AddI18N(lang = "cn", value = "平均触发数", code = "ACPU")
  })
  @Query(value =
    "SELECT UNIT_TIME(epoch, #{utime}) AS utime, COUNT(NAME)/COUNT(DISTINCT SID) AS acpu " +
      "FROM EVENTS_CESSIONS GROUP BY utime #{group} #{having}",
    nativeQuery = true)
  @Transactional
  public Page<?> clicksPerUser(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 平均交互深度: 衡量完成关键操作之前所经深度，当然是越浅越好
  // 定义：等于 Session 内事件数之和除以总的 Session 数
  // 如何定义所有 Session ???
  // TODO: 注意排除非用户主动触发事件, 是否需要在采集端做标识（注意不要和target混淆）
  @I18N({
    @AddI18N(lang = "cn", value = "平均交互深度", code = "AvgReactDepth")
  })
  @Query(value =
    "", nativeQuery = true)
  Page<?> reactDepths(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 平均使用时长: 搭配平均交互深度，反映完成关键操作所需时间
  // 定义: 等于所有用户的 Session 时长之和除以 Session 数
  @I18N({
    @AddI18N(lang = "cn", value = "平均使用时长", code = "AvgReactDuration")
  })
  @Query(value = "SELECT UNIT_TIME(epoch, :utime) as unitTime, sid, " +
    "(LAST_VALUE(epoch) WITHIN GROUP (ORDER BY epoch ASC) - FIRST_VALUE(epoch) WITHIN GROUP (ORDER BY epoch ASC)) AS duration FROM INSPIRER.EVENTS " +
    "WHERE epoch >= :startAt AND epoch <= :endAt " +
    "GROUP BY unitTime, sid", nativeQuery = true)
  Page<?> reactDurations(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 页面平均停留时长
  // 定义: 等于页面停留时长的总和除以页面被浏览的触发数
  @I18N({
    @AddI18N(lang = "cn", value = "平均停留时长", code = "AvgStayDuration")
  })
  @Query(value = "SELECT UNIT_TIME(epoch, :utime) as unitTime, sid, page.gid, " +
    "(LAST_VALUE(epoch) WITHIN GROUP (ORDER BY epoch ASC) - FIRST_VALUE(epoch) WITHIN GROUP (ORDER BY epoch ASC)) AS duration FROM INSPIRER.EVENTS " +
    "WHERE epoch >= :startAt AND epoch <= :endAt " +
    "GROUP BY unitTime, sid, page.gid", nativeQuery = true)
  Page<?> stayDurations(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 跳出率
  // 定义: 当一个 Session 仅有一个事件时, 即视为跳出
  // 一般情况这个事件以浏览页面居多, 所以 Session 整体跳出率等于跳出的 Session 数除以 Session 总数
  // 而具体事件或页面的跳出率, 可以按属性查看或筛选得出
  @I18N({
    @AddI18N(lang = "cn", value = "跳出率", code = "BounceRate")
  })
  @Query(value = "SELECT COUNT(CASE WHEN eventCount=1 THEN eventCount END) AS number, SUM(eventCount) AS total FROM " +
    " (SELECT UNIT_TIME(epoch, :utime) as unitTime, sid, COUNT(name) AS eventCount " +
    " WHERE epoch >= :startAt AND epoch <= :endAt " +
    " GROUP BY unitTime, sid)", nativeQuery = true)
  Page<?> bounceRate(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 退出率
  // 定义: 当用户在`某个页面`结束了该 Session 时即视为退出, 所以页面退出率等于退出的页面数除以该页面的总浏览次数
  // 因而, 退出是相对某个页面而言
  @I18N({
    @AddI18N(lang = "cn", value = "退出率", code = "ExitRate")
  })
  @Query(value = "SELECT COUNT(DISTINCT page.referrer) AS number, COUNT(page.greferrer) AS total FROM INSPIRER.EVENTS " +
    "WHERE page.greferrer IN " +
    " (SELECT LAST_VALUE(page.greferrer) WITHIN GROUP (ORDER BY epoch DESC) AS referrer FROM INSPIRER.EVENTS " +
    " WHERE epoch >= :startAt AND epoch <= :endAt " +
    " GROUP BY unitTime, sid) " +
    "AND epoch >= :startAt AND epoch <= :endAt " +
    "GROUP BY unitTime, sid", nativeQuery = true)
  Page<?> exitRate(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(nativeSql, pageable);
    return result;
  }

  // 新用户留存数 New User Retention
  // 定义: 在互联网行业中, 用户在某段时间内开始使用应用, 经过一段时间后, 仍然继续使用该应用的用户, 被认作是留存用户
  // 这部分用户占当时新增用户的比例即是留存率, 会按照每隔1单位时间 (例日, 周, 月) 来进行统计
  // 顾名思义, 留存指的就是 "有多少用户留下来了"
  // 用户复访率
  // 用户复访次数
  @I18N({
    @AddI18N(lang = "cn", value = "新用户留存数")
  })
  @Query(value = "CREATE VIEW IF NOT EXISTS INSPIRER.USERS_VIEW " +
    "AS SELECT UNIT_TIME(epoch, :utime) AS unitTime, sid FROM INSPIRER.CESSIONS WHERE epoch >= :startAt AND epoch <= :endAt + TO_DAY(:utime); " +
    "SELECT fromUnitTime, toUnitTime, COUNT(DISTINCT sid) FROM " +
    " (SELECT unitTime as fromUnitTime, toUnit.unitTime as toUnitTime, toUnit.sid as sid FROM " +
    "   (SELECT unitTime, FIRST_VALUES(sid, 10000000) WITHIN GROUP (ORDER BY epoch ASC) AS sids FROM INSPIRER.USERS_VIEW " +
    "   WHERE epoch >= :startAt AND epoch <=: endAt AND sid NOT IN " +
    "     (SELECT sid FROM INSPIRER.CESSIONS WHERE epoch < :startAt) " +
    "   GROUP BY unitTime) AS fromUnit " +
    " LEFT JOIN INSPIRER.USERS_VIEW AS toUnit " +
    " ON toUnit.unitTime <= fromUnit.unitTime + TO_DAY(:utime) WHERE toUnit.sid = ANY(fromUnit.sids)) " +
    "GROUP BY fromUnitTime, toUnitTime", nativeQuery = true)
  Page<?> userRetentionRate(BaseHandler handler, String where, String groupBy, String sql, HttpServletRequest request, Pageable pageable) throws SQLException {
    Page<?> result = doQuery.queryBySql(sql, pageable);
    return result;
  }

}
