/**
 * Created by 杭盖 on 2018/09/09.
 *
 */
package com.godalgo.inspirer.cloud.analytics.repository;


import com.godalgo.inspirer.cloud.analytics.annoation.ContextFactoryMethod;
import com.godalgo.inspirer.cloud.analytics.handler.BaseHandler;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;

@Repository
public class IndicatorRepository {
  @Autowired
  private QueryImpl doQuery;

  // 指标设计器中的因子: 前置事件、主体事件、后置事件以及对比事件
  // 指标体现的是`均值`概念, 比如: 人均值
  // 分子衡量指标主体, 分母衡量参与个体, 体现为: 主体/个体
  // 分子、分母也可以是指标
  // 约定: 凡是涉及对比, 必须保证参与对象先决条件一致, 否则对比没有意义; 即: 指标的分子、分母共享条件
  // 指标是针对业务而言, 因此前台会屏蔽byType选项, 即TEXT都是NODE_TEXT
  @Query(value =
    "SELECT UNIT_TIME(EPOCH,#{utime}) DATE,#{dividend}/#{divisor} FROM (" +
      "(" +
        // 前置事件
        "(SELECT SID,EPOCH FROM EVENTS_CESSIONS(ELEMENT.ga_NodeText VARCHAR) WHERE ELEMENT.ga_NodeText='#{preEvent}') A " +
        "INNER JOIN " +
        // 主体事件
        "(SELECT SID,EPOCH FROM EVENTS_CESSIONS(ELEMENT.ga_NodeText VARCHAR) WHERE ELEMENT.ga_NodeText='#{event}') B " +
        // 单位时间内, 主体事件时间戳大于前置事件时间戳
        "ON A.SID=B.SID AND UNIT_TIME(A.EPOCH,#{utime})=UNIT_TIME(B.EPOCH,#{utime}) AND A.EPOCH<B.EPOCH" +
      ") C " +
      "INNER JOIN " +
      // 后置事件
      "(SELECT SID,EPOCH FROM EVENTS_CESSIONS(ELEMENT.ga_NodeText VARCHAR) WHERE ELEMENT.ga_NodeText='#{postEvent}') D " +
      // 单位时间内, 主体事件时间戳小于后置事件时间戳
      "ON C.SID=D.SID AND UNIT_TIME(C.EPOCH,#{utime})=UNIT_TIME(D.EPOCH,#{utime}) C.EPOCH<D.EPOCH) E(#{dynamicColumnsWithType}) " +
    "GROUP BY DATE #{group}", nativeQuery = true)
  @ContextFactoryMethod(method = "indicator")
  @Transactional
  public Object indicator(BaseHandler handler, String nativeSql, Pageable pageable) throws SQLException {
    Object rs = (pageable != null) ? doQuery.queryBySql(nativeSql, pageable) : doQuery.queryBySql(nativeSql);
    return rs;
  }

}
