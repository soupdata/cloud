/**
 * Created by 杭盖 on 2018/09/09.
 *
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
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.util.List;


//@RepositoryRestResource(collectionResourceRel = "event", path = "event")
@Repository
public class StepRepository {
  @Autowired
  private QueryImpl doQuery;

  // 科普
  // 行为序列和用户的关系是两个不同的观察维度
  // 若聚焦行为序列，则在同一单位时间内同一用户反复重复既定的行为序列，应计算为多次，而非1次；若聚焦用户，则计算为1次
  // 不同单位时间内的同一用户的行为序列不在此列，独立计算

  // 漏斗
  // 计算漏斗单位时间内某步骤转化, 支持分组, 支持最多10步
  // 如果一个用户在所选时段内有多个事件都符合某个转化步骤的定义，
  // 那么会优先选择更靠近最终转化目标的事件作为转化事件，并在第一次达到最终转化目标时停止转化的计算
  // 步骤倒推
  // 例 1：访问首页 -> 选择支付方式（支付宝） -> 选择支付方式（微信）-> 支付成功
  // 例 2：访问首页 -> 选择支付方式（支付宝） -> 访问首页 -> 选择支付方式（微信）-> 支付成功
  // 例 3：访问首页 -> 选择支付方式（支付宝） -> 访问首页 -> 选择支付方式（微信）-> 支付成功 -> 选择支付方式（微信）-> 支付成功
  // 同组, 即意味着同组属性具备关联性
  // 理论上, 组属性属于同一个会话, 但从使用场景来看, 可假设后续每一步骤同起点组属性
  // 使用了GROUP BY之后, SQL字段的传递性`貌似`就断掉了, 因为GROUP BY要求查询字段必须出现在GROUP BY中
  // ucd: User Centered Design

  // 留存
  // 留存本质上是只包含起点与终点的漏斗, 且ucd=true, 语义重点在于多少人留下来了

  // 聚合的标准范例
  @Query(value =
    "UPSERT INTO VIEW_STEPS(ID, SID, ORIGIN, START_EPOCH, END_EPOCH, REST #{viewDynamicColumnsWithType}, UUID) " +
    "SELECT NEXT VALUE FOR VIEW_STEPS_ID, SESSION_ID, ORIGIN, START_EPOCH, " +
      "NTH_VALUE(STEP_TIME, #{restSize}) WITHIN GROUP ( ORDER BY STEP_TIME ASC) END_EPOCH, " +
      "FIRST_VALUES(STEP, #{restSize}) WITHIN GROUP (ORDER BY STEP_TIME ASC) REST #{groupAlias}, '#{uuid}' FROM " +
      "(SELECT SESSION_ID, ORIGIN, START_EPOCH, STEP, FIRST_VALUE (B_EPOCH) WITHIN GROUP (ORDER BY B_EPOCH ASC) STEP_TIME FROM " +
        "(SELECT LAST_VALUE (A.SID) WITHIN GROUP (ORDER BY B.EPOCH ASC) SESSION_ID, " +
                "LAST_VALUE (A.TEXT) WITHIN GROUP ( ORDER BY B.EPOCH ASC) ORIGIN, " +
                "LAST_VALUE (A.EPOCH) WITHIN GROUP (ORDER BY B.EPOCH ASC) START_EPOCH, " +
                "LAST_VALUE (B.TEXT) WITHIN GROUP (ORDER BY B.EPOCH ASC) STEP, " +
                "B.EPOCH B_EPOCH #{groupAggs} FROM " +
          "(" +
            "(SELECT SID, ELEMENT.GA_NODETEXT TEXT, EPOCH #{dynamicColumns} FROM EVENTS_CESSIONS(ELEMENT.GA_NODETEXT VARCHAR #{dynamicColumnsWithType}) WHERE ELEMENT.GA_NODETEXT='#{param.get('start')}') A " +
            "LEFT JOIN " +
            "(SELECT SID, ELEMENT.GA_NODETEXT TEXT, EPOCH FROM EVENTS_CESSIONS(ELEMENT.GA_NODETEXT VARCHAR) WHERE ELEMENT.GA_NODETEXT=ANY(#{rest.get('array')})) B " +
            "ON A.SID=B.SID" +
          ") WHERE B.EPOCH>A.EPOCH GROUP BY B.EPOCH" + // 任何一个步骤, 只会发生一次, 对应唯一时间点; 因此处为LEFT JOIN, 所以取满足B.EPOCH>A.EPOCH条件的最后一个值(之前都是join值而已)
        ") " +
      "GROUP BY SESSION_ID, ORIGIN, START_EPOCH, STEP) " + // 以`漏斗单元`去重rest step, FIRST IN LAST OUT
    "GROUP BY SESSION_ID, ORIGIN, START_EPOCH #{groupAlias}", // 得到一个包含分组信息的漏斗描述信息单元
    countQuery =
    "SELECT PERCENT(A.VAL, B.TOTAL, 2) VAL, A.DATE DATE #{viewAlias} FROM " +
      "(SELECT #if(!$param.getOrDefault('ucd','').equals('1')) COUNT(DISTINCT SID) #else COUNT(SID) #end VAL, " +
        "UNIT_TIME(END_EPOCH, #{utime}) DATE #{viewDynamicColumnsWithAlias} FROM " + // UNIT_TIME(END_EPOCH), 从截止时间向前反推窗口期
        "VIEW_STEPS(GROUPBY.PLACEHODLER VARCHAR #{viewDynamicColumnsWithType}) " +
        "WHERE ARRAY_TO_STRING(REST, ',') LIKE '#{rest.get('join')}%' " +
        "GROUP BY DATE #{viewDynamicColumns}) A " +
      "INNER JOIN " +
      "(SELECT #if(!$param.getOrDefault('ucd','').equals('1')) COUNT(DISTINCT SID) #else COUNT(SID) #end TOTAL, " +
        "UNIT_TIME(END_EPOCH, #{utime}) DATE FROM " +
        "VIEW_STEPS(GROUPBY.PLACEHODLER VARCHAR #{viewDynamicColumnsWithType}) " +
        "GROUP BY DATE #{viewDynamicColumns}) B " +
      "ON A.DATE=B.DATE"
    , name = "VIEW_STEPS", nativeQuery = true)
  @ContextFactoryMethod(method = "step", params = "{\"orderBy\": \"B.EPOCH\", \"viewAliasPrefix\": \"A\"}")
  @Transactional
  public Object step(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Object rs = (pageable != null) ? doQuery.queryBySql(nativeSql, pageable) : doQuery.queryBySql(nativeSql);
    return rs;
  }

  // 计算漏斗所有步骤在时间范围内的总转化率, 不限制窗口期
  @Querys(value = {
    @Query(value = "UPSERT INTO VIEW_STEPS(ID, SID, ORIGIN, START_EPOCH, END_EPOCH, REST, UUID) " +
      "SELECT NEXT VALUE FOR VIEW_STEPS_ID, SESSION_ID, ORIGIN, START_EPOCH, " +
        "NTH_VALUE(STEP_TIME, #{restSize}) WITHIN GROUP ( ORDER BY STEP_TIME ASC) END_EPOCH, " +
        "FIRST_VALUES(STEP, #{restSize}) WITHIN GROUP (ORDER BY STEP_TIME ASC) REST, '#{uuid}' FROM " +
        "(SELECT SESSION_ID, ORIGIN, START_EPOCH, STEP, FIRST_VALUE (B_EPOCH) WITHIN GROUP (ORDER BY B_EPOCH ASC) STEP_TIME FROM " +
          "(SELECT LAST_VALUE (A.SID) WITHIN GROUP (ORDER BY B.EPOCH ASC) SESSION_ID, " +
                  "LAST_VALUE (A.TEXT) WITHIN GROUP ( ORDER BY B.EPOCH ASC) ORIGIN, " +
                  "LAST_VALUE (A.EPOCH) WITHIN GROUP (ORDER BY B.EPOCH ASC) START_EPOCH, " +
                  "LAST_VALUE (B.TEXT) WITHIN GROUP (ORDER BY B.EPOCH ASC) STEP, " +
                  "B.EPOCH B_EPOCH #{groupAggs} FROM " +
          "(" +
            "(SELECT SID, ELEMENT.GA_NODETEXT TEXT, EPOCH FROM EVENTS_CESSIONS(ELEMENT.GA_NODETEXT VARCHAR) WHERE ELEMENT.GA_NODETEXT='#{param.get('start')}') A " +
            "LEFT JOIN " +
            "(SELECT SID, ELEMENT.GA_NODETEXT TEXT, EPOCH FROM EVENTS_CESSIONS(ELEMENT.GA_NODETEXT VARCHAR) WHERE ELEMENT.GA_NODETEXT=ANY(#{rest.get('array')})) B " +
            "ON A.SID=B.SID" +
            ") WHERE B.EPOCH>A.EPOCH GROUP BY B.EPOCH" +
          ") " +
          "GROUP BY SESSION_ID, ORIGIN, START_EPOCH, STEP) " +
        "GROUP BY SESSION_ID, ORIGIN, START_EPOCH", name = "VIEW_STEPS", nativeQuery = true),
    @Query(value =
      "SELECT #if(!$param.getOrDefault('ucd','').equals('1')) COUNT(DISTINCT SID) #else COUNT(SID) #end TOTAL FROM VIEW_STEPS",
      countQuery =
        "SELECT A.VAL VAL, #{counter.get('TOTAL')} TOTAL FROM (" +
          "#foreach ($restJoin in $restJoins) " +
          "#if($velocityCount!=1) UNION ALL #end " +
          "SELECT #if(!$param.getOrDefault('ucd','').equals('1')) COUNT(DISTINCT SID) #else COUNT(SID) #end VAL FROM VIEW_STEPS " +
          "WHERE ARRAY_TO_STRING(REST, ',') LIKE '#{restJoin}%'" +
          "#end) A", countName = "counter", nativeQuery = true)
  })
  @ContextFactoryMethod(method = "step")
  //@Transactional(propagation= Propagation.REQUIRES_NEW, isolation = Isolation.READ_UNCOMMITTED)
  public Object steps(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Object rs = (pageable != null) ? doQuery.queryBySql(nativeSql, pageable) : doQuery.queryBySql(nativeSql);
    // 中间表利用完成后, 删除此次批次数据
    return rs;
  }

}
