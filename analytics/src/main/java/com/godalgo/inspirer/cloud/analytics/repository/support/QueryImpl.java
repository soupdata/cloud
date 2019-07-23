package com.godalgo.inspirer.cloud.analytics.repository.support;

/**
 * Created by 杭盖 on 2018/1/6.
 */

import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Selection;
import javax.persistence.metamodel.EntityType;

import com.godalgo.inspirer.cloud.analytics.AnalyticsApp;
import lombok.extern.slf4j.Slf4j;
import com.google.common.base.Strings;

import org.hibernate.Session;
import org.hibernate.query.NativeQuery;
import org.hibernate.transform.Transformers;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

/**
 * QueryImpl 类实现了 IQuery<br>
 */
@SuppressWarnings({"unchecked", "rawtypes"})
@Slf4j
@Component(value="doQuery")
// 为何继承EntityManagerFactoryProxy
public class QueryImpl<T> implements IQuery {
/*  // EntityManagerFactoryImpl 没有以 @Service 或 @Component 注解, 所以无法获取
  @Resource(name="entityManagerFactory")
  private EntityManagerFactory entityManagerFactory;*/

  /**
   * 每次批量操作数
   */
  private int batchSize = 50;

  public QueryImpl() {
//    EntityManagerFactory entityManagerFactory = AnalyticsApp.getBean("entityManagerFactory");
//    this.setEmf(entityManagerFactory);
//    EntityManager em = getEntityManager();
//    if (em == null) {
//      em = entityManagerFactory.createEntityManager();
//    }
//    this.setEntityManager(em);
  }

  // spring-style
  public EntityManager getEntityManager() {
    Map<String, T> beans = AnalyticsApp.getBean((Class<T>) EntityManager.class);
    String[] keys = beans.keySet().toArray(new String[0]);
     return (EntityManager) beans.get(keys[0]);
  }

  /**
   * 设置每次操作数
   */
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public <E> E get(Class clazz, Serializable id) {
    return (E) getEntityManager().find(clazz, id);
  }

  /**
   * 插入记录
   *
   * @param entity 要插入的记录
   */
  public void insert(Object entity) {
    if (entity instanceof List) {
      insertList((List) entity);
      return;
    } else if (entity instanceof Object[]) {
      return;
    }
    try {
      getEntityManager().persist(entity);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 批量增加
   *
   * @param list 要新增的数据
   */
  public void insertList(List list) {
    EntityManager entityManager = getEntityManager();
    if (list == null || list.size() == 0) {
      return;
    }
    int i = 0;
    for (Object o : list) {
      insert(o);
      if (i % batchSize == 0) {
        entityManager.flush();
      }
      i++;
    }
  }

  /**
   * 更新记录
   *
   * @param entity 要更新的记录
   */
  public void update(Object entity) {
    if (entity instanceof List) {
      this.updateList((List) entity);
      return;
    }
    getEntityManager().merge(entity);
  }

  /**
   * 更新list
   */
  public void updateList(List list) {
    for (Object entity : list) {
      this.update(entity);
    }
  }

  /**
   * 删除记录
   *
   * @param entity 要删除的记录
   */
  public void delete(Object entity) {
    if (entity instanceof List) {
      List list = (List) entity;
      for (Object o : list) {
        getEntityManager().remove(o);
      }
    } else {
      getEntityManager().remove(entity);
    }
  }

  public <E extends Serializable> List<E> query(String jpql) {
    return getEntityManager().createQuery(jpql).getResultList();
  }

  public Integer updateJpql(String jpql) {
    return getEntityManager().createQuery(jpql).executeUpdate();
  }

  public int updateSql(String sql) {
    return getEntityManager().createNativeQuery(sql).executeUpdate();
  }

  public int updateSql(String sql, Session session) {
    if (session != null) {
      return session.createNativeQuery(sql).executeUpdate();
    } else {
      return getEntityManager().createNativeQuery(sql).executeUpdate();
    }

  }

  public <E extends Serializable> Page<E> queryBySql(String sql, Pageable pageable) throws SQLException {
    String countSql = "SELECT COUNT(*) from (" + sql + ")";
    javax.persistence.Query query = getEntityManager().createNativeQuery(sql);
    int pageSize = pageable.getPageSize();
    int pageNumber = pageable.getPageNumber();
    query.setFirstResult(pageNumber * pageSize);
    query.setMaxResults(pageSize);
    query.unwrap(NativeQuery.class).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP);
    List list = query.getResultList();
    return new PageImpl(list, pageable, pageSize);
  }

  public <E extends Serializable> List<E> queryBySql(String sql) {
    javax.persistence.Query query = getEntityManager().createNativeQuery(sql);
    query.unwrap(NativeQuery.class).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP);
    return query.getResultList();
  }

  public <E extends Serializable> List<E> queryBySql(String sql, Session session) {
    javax.persistence.Query query = null;
    if (session != null) {
      query = session.createNativeQuery(sql);
    } else {
      query = getEntityManager().createNativeQuery(sql);
    }

    query.unwrap(NativeQuery.class).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP);
    return query.getResultList();
  }
  /**
   * 查询记录
   *
   * @param clazz        要查询的实体类
   * @param hqlCondition 查询条件
   */
  public <E extends Serializable> List<E> query(Class clazz, String hqlCondition) {
    return getEntityManager().createQuery("select t from " + clazz.getName() + " as t where " + hqlCondition)
      .getResultList();
  }

  public void delete(Class entity, String jpqlCondition) {
    if (Strings.isNullOrEmpty(jpqlCondition)) {
      jpqlCondition = "1=1";
    }
    int no = updateJpql("delete " + entity.getName() + " where " + jpqlCondition);
  }

  /**
   * 根据ids删除数据
   *
   * @param entity 删除实体类
   * @param ids    删除条件
   */
  public void delete(Class entity, List ids) {
    String idName = getIdName(entity, getEntityManager());
    StringBuffer sb = new StringBuffer();
    sb.append(idName + " in(");
    for (int i = 0; i < ids.size(); i++) {
      sb.append("'" + ids.get(i) + "',");
    }
    String jpqlCondition = sb.substring(0, sb.length() - 1) + ")";
    delete(entity, jpqlCondition);
  }

  public <E extends Serializable> List<E> query(String jpql, int firstResult, int maxResults) {
    List result = getEntityManager().createQuery(jpql).setFirstResult(firstResult).setMaxResults(maxResults)
      .getResultList();
    return result;
  }

  public <E extends Serializable> List<E> queryBySql(String sql, int firstResult, int maxResults) {
    return getEntityManager().createNativeQuery(sql).setFirstResult(firstResult).setMaxResults(maxResults)
      .getResultList();
  }

  public <E extends Serializable> List<E> queryAll(Class clazz) {
    CriteriaQuery criteriaQuery = getEntityManager().getCriteriaBuilder().createQuery(clazz);
    criteriaQuery.from(clazz);
    return getEntityManager().createQuery(criteriaQuery).getResultList();
  }

  public QueryPage queryPageByJpql(String jpql, int pageNo, int rowsPerPage) {
    if (pageNo <= 0)
      pageNo = 1;
    if (rowsPerPage <= 0)
      rowsPerPage = 7;
    String countJpql = "select count(*) from (" + jpql + ")";
    int count = getCount(countJpql).intValue();
// 当把最后一页数据删除以后,页码会停留在最后一个上必须减一
    int totalPageCount = count / rowsPerPage;
    if (pageNo > totalPageCount && (count % rowsPerPage == 0)) {
      pageNo = totalPageCount;
    }
    if (pageNo - totalPageCount > 2) {
      pageNo = totalPageCount + 1;
    }
    int firstResult = (pageNo - 1) * rowsPerPage;
    if (firstResult < 0) {
      firstResult = 0;
    }
    List result = getEntityManager().createQuery(jpql).setFirstResult(firstResult).setMaxResults(rowsPerPage)
      .getResultList();
    return new QueryPage(count, pageNo, rowsPerPage, result);
  }

  public Long getCount(String jpql) {
    return (Long) getEntityManager().createQuery(jpql).getResultList().get(0);
  }

  /***
   *
   * @Method updateJpql
   * @Description 根据传入的带有占位符的sql语句, 做增删改操作 例如
   * updateJpql("update user t set t.name=? where t.id=?"
   * ,{[zx],[23]})
   * @param jpql
   * 占位符式的sql
   * @param paramList
   * list里面装有[zx, 23]
   */
  public void updateJpql(String jpql, List paramList) {
    javax.persistence.Query query = getEntityManager().createQuery(jpql);
    for (int i = 0; i < paramList.size(); i++) {
      query.setParameter(i + 1, paramList.get(i));
    }
    query.executeUpdate();
  }

  /**
   * 统计记录
   *
   * @param query 统计条件
   */
  public Long getCount(Query query) {
    Selection selection = query.getCriteriaQuery().getSelection();
    query.getCriteriaQuery().select(query.getCriteriaBuilder().count(query.getFrom()));
    Long count = (Long) getEntityManager().createQuery(query.newCriteriaQuery()).getResultList().get(0);
    query.getCriteriaQuery().select(selection);
    return count;
  }

  /**
   * 分页查询
   *
   * @param query       查询条件
   * @param pageNo      页号
   * @param rowsPerPage
   */
  public QueryPage queryPage(Query query, int pageNo, int rowsPerPage) {
    if (pageNo <= 0)
      pageNo = 1;
    if (rowsPerPage <= 0)
      rowsPerPage = 7;

//    for (Predicate cri : query.getPredicates())
//      log.debug(cri.toString());
    int count = getCount(query).intValue();
// 当把最后一页数据删除以后,页码会停留在最后一个上必须减一
    int totalPageCount = count / rowsPerPage;
    if (pageNo > totalPageCount && (count % rowsPerPage == 0)) {
      pageNo = totalPageCount;
    }
    if (pageNo - totalPageCount > 2) {
      pageNo = totalPageCount + 1;
    }
    int firstResult = (pageNo - 1) * rowsPerPage;
    if (firstResult < 0) {
      firstResult = 0;
    }
    List result = getEntityManager().createQuery(query.newCriteriaQuery()).setFirstResult(firstResult)
      .setMaxResults(rowsPerPage).getResultList();
    return new QueryPage(count, pageNo, rowsPerPage, result);
  }

  /**
   * 根据query查找记录
   *
   * @param query       查询条件
   * @param firstResult 起始行
   * @param maxResults  结束行
   */
  public <E extends Serializable> List<E> query(Query query, int firstResult, int maxResults) {
    List result = getEntityManager().createQuery(query.newCriteriaQuery()).setFirstResult(firstResult)
      .setMaxResults(maxResults).getResultList();
    return result;
  }

  /**
   * 根据query查找记录
   *
   * @param query 查询条件
   */
  public <E extends Serializable> List<E> query(Query query) {
    return getEntityManager().createQuery(query.newCriteriaQuery()).getResultList();
  }

  /**
   * 获得主键名称
   *
   * @param clazz         操作是实体对象
   * @param entityManager jpa的entityManager 工厂
   * @return 初建名称
   */
  public static String getIdName(Class clazz, EntityManager entityManager) {
    EntityType entityType = entityManager.getMetamodel().entity(clazz);
    return entityType.getId(entityType.getIdType().getJavaType()).getName();
  }
}

