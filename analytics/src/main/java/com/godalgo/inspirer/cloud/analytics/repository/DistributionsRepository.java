/**
 * Created by 杭盖 on 2018/09/09.
 *
 */
package com.godalgo.inspirer.cloud.analytics.repository;


import com.godalgo.inspirer.cloud.analytics.annoation.ContextFactoryMethod;
import com.godalgo.inspirer.cloud.analytics.annoation.Querys;
import com.godalgo.inspirer.cloud.analytics.handler.BaseHandler;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.List;


@Repository
public class DistributionsRepository {
  @Autowired
  private QueryImpl doQuery;

  // 正态分布
  // 分布解决的问题有三类:
  // A. 请问 90% 的 订单均价 (1期)
  // B. 请问 订单均价 为 90 的 百分占比
  // C. 请问 订单均价在 90~100 区间内的 用户占比 (2期) <- 聚焦用户
  // 默认支持 6 等分 (0.1, 0.3, 0.5, 0.7, 0.9)，可考虑支持自定义等分 (8, 2)

  // A
  @Query(value = "SELECT UNIT_TIME(EPOCH, #{utime}) DATE, " +
    "PERCENTILE_CONT(#{param.get('percentile')}) WITHIN GROUP (ORDER BY #{column} DESC) VAL " +
    "FROM EVENTS_CESSIONS GROUP BY DATE" , nativeQuery = true)
  List<Integer> contDistribution(BaseHandler handler, String nativeSql, Pageable pageable) {
    return doQuery.queryBySql(nativeSql);
  }

  // B
  // 需预先获取订单区间, 2期
  @Query(value = "SELECT UNIT_TIME(EPOCH, #{utime}) DATE, " +
    "PERCENT_RANK(#{param.get('endrange')}) WITHIN GROUP (ORDER BY #{column} DESC) VAL " +
    "FROM EVENTS_CESSIONS GROUP BY DATE" , nativeQuery = true)
  List<Integer> rankDistribution(BaseHandler handler, String nativeSql, Pageable pageable) {
    return doQuery.queryBySql(nativeSql);
  }

  // C
  // 需预先获取订单区间, 2期
  // 范围不包括左边界
  @Query(value =
    "SELECT PERCENT(A.VAL, B.TOTAL, 2) VAL, A.DATE DATE FROM (" +
      "(SELECT COUNT(DISTINCT SID) VAL, UNIT_TIME(EPOCH, #{utime}) DATE FROM EVENTS_CESSIONS " +
      "WHERE #{column} > #{param.get('startrange')} AND #{column} <= #{param.get('endrange')} " +
      "GROUP BY DATE) A " +
      "INNER JOIN " +
      "(SELECT COUNT(DISTINCT SID) TOTAL, UNIT_TIME(EPOCH, #{utime}) DATE FROM EVENTS_CESSIONS " +
      "GROUP BY DATE) B " +
      "ON A.DATE=B.DATE" +
    ")" , nativeQuery = true)
  List<Integer> userDistribution(BaseHandler handler, String nativeSql, Pageable pageable) {
    return doQuery.queryBySql(nativeSql);
  }
}
