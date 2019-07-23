package com.godalgo.inspirer.cloud.analytics.repository.support;

/**
 * Created by 杭盖 on 2018/1/6.
 */

import org.springframework.data.repository.NoRepositoryBean;

import javax.persistence.EntityManager;
import java.io.Serializable;
import java.util.List;

/**
 * IQuery基类<br>
 *
 * @describe：系统基础 JPA DAO 接口
 */
@SuppressWarnings({ "rawtypes" })
@NoRepositoryBean
public interface IQuery {
  public EntityManager getEntityManager();
  public <E> E get(Class clazz, Serializable id);
  /**
   * 插入记录
   *
   * @param entity 要插入的记录
   */
  public void insert(Object entity);
  /**
   * 更新记录
   *
   * @param entity 要更新的记录
   */
  public void update(Object entity);
  /** 更新list */
  public void updateList(List list);
  /**
   * 删除记录
   *
   * @param entity 要删除的记录
   */
  public void delete(Object entity);
  /**
   * 删除记录
   *
   * @param entity 要删除的记录
   */
  public void delete(Class entity, List ids);
  /**
   * 删除记录
   *
   * @param entity 要删除的记录
   */
  public void delete(Class entity, String jpqlCondition);
  /**
   * 统计记录
   *
   * @param query 统计条件
   */
  public Long getCount(Query query);
  public Long getCount(String jpql);
  /**
   * 分页查询
   *  @param query 查询条件
   * @param pageNo 页号
   * @param rowsPerPage 每页显示条数
   */
  public QueryPage queryPage(Query query, int pageNo, int rowsPerPage);
  /**
   * 根据query查找记录
   *
   * @param query 查询条件
   * @param firstResult 起始行
   * @param maxResults 结束行
   */
  public <E extends Serializable> List<E> query(Query query, int firstResult, int maxResults);
  /**
   * 根据query查找记录
   *
   * @param query 查询条件
   */
  public <E extends Serializable> List<E> query(Query query);
  /**
   * 执行更新操作的jpql语句
   *
   * @param jpql 要执行的jpql语句
   */
  public <E extends Serializable> List<E> query(String jpql);
  public <E extends Serializable> List<E> queryAll(Class clazz);
  public <E extends Serializable> List<E> query(String jpql, int firstResult, int maxResults);
  /**
   * 执行查询操作的sql语句
   *
   * @param sql 要执行的sql语句
   */
  public <E extends Serializable> List<E> queryBySql(String sql);
  public <E extends Serializable> List<E> queryBySql(String sql, int firstResult, int maxResults);
  /**
   * 查询记录
   *
   * @param clazz 要查询的实体类
   * @param hqlCondition 查询条件
   */
  public <E extends Serializable> List<E> query(Class clazz, String hqlCondition);
  /**
   * 执行更新操作的sql语句
   *
   * @param sql 要执行的sql语句
   */
  public int updateSql(String sql);
  public Integer updateJpql(String jpql);
  public QueryPage queryPageByJpql(String hql, int pageNo, int rowsPerPage);
  public void updateJpql(String jpql, List paramList);
}
