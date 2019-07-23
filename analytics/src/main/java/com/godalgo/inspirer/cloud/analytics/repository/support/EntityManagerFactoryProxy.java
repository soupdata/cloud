package com.godalgo.inspirer.cloud.analytics.repository.support;

/**
 * Created by 杭盖 on 2018/1/6.
 */
import java.util.Collection;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
/**
 * EntityManager 管理器
 * 使用 ThreadLocal 来保证 EntityManager 的线程安全
 *
 */
public class EntityManagerFactoryProxy {
  private static final ThreadLocal<EntityManager> emThreadLocal = new InheritableThreadLocal<EntityManager>();
  private static EntityManagerFactory emf;
  public void setEmf(EntityManagerFactory emf) {
    EntityManagerFactoryProxy.emf = emf;
  }
  public static EntityManagerFactory getEmf() {
    return emf;
  }
  public EntityManager getEntityManager() {
    return emThreadLocal.get();
  }
  public void setEntityManager(EntityManager em) {
    emThreadLocal.set(em);
  }
  /**
   * 创建查询条件
   *
   * @param name 字段名称
   * @param values 字段值
   */
  public String createInCondition(String name, Collection<String> values) {
    if (values == null || values.size() == 0) {
      return "1<>1";
    }
    StringBuffer sb = new StringBuffer();
    sb.append(name + " in(");
    for (String id : values) {
      sb.append("'" + id + "',");
    }
    String hsqlCondition = sb.substring(0, sb.length() - 1) + ")";
    return hsqlCondition;
  }
}
