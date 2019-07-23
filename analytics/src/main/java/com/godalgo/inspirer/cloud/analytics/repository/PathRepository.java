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


//@RepositoryRestResource(collectionResourceRel = "event", path = "event")
@Repository
public class PathRepository {
  @Autowired
  private QueryImpl doQuery;

  // 智能路径, 与 Session 相关
  @Query(value = "SELECT b.sid, c.name, c.epoch FROM " +
    " ((SELECT sid FROM INSPIRER.EVENTS " +
    " WHERE name=ANY(REGEXP_SPLIT(:paths, ',')) AND sid IN (" +
    "   SELECT sid FROM INSPIRER.EVENTS " +
    "   WHERE name = :name AND epoch >= :startAt AND epoch <= :endAt" +
    " )) AS b " +
    " LEFT JOIN (SELECT name FROM INSPIRER.EVENTS WHERE epoch >= :startAt AND epoch <= :endAt) AS c " +
    " ON b.sid=c.sid) AS a", nativeQuery = true)
  List<Integer> paths(String table, String where, String groupBy, @Nullable String sql) {
    return doQuery.queryBySql(sql);
  }

}
