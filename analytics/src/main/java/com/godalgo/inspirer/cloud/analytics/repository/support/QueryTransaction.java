package com.godalgo.inspirer.cloud.analytics.repository.support;

/**
 * Created by 杭盖 on 2018/1/6.
 */

import javax.annotation.Resource;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.stereotype.Component;

/**
 * @describe JPA事务管理
 */
@Slf4j
@Component(value = "doTransaction")
public class QueryTransaction {
  private String[] txmethod;// 配置事务的传播特性方法
//  @Resource
  private EntityManagerFactory entityManagerFactory;// JPA工厂
//  @Around("execution(public * com.godalgo.inspirer.cloud.analytics.repository..*.*(..))")
  public Object exec(ProceedingJoinPoint point) throws Throwable {
    Signature signature = point.getSignature();

    Boolean isTransaction = false;
    for (String method : txmethod) {
      if (signature.getName().startsWith(method)) {// 以method开头的方法打开事务
        isTransaction = true;
        break;
      }
    }
// JPA->Hibernate
    if (point.getTarget() instanceof EntityManagerFactoryProxy) {
// 获得被代理对象
      EntityManagerFactoryProxy emfp = (EntityManagerFactoryProxy) point.getTarget();
      EntityManager em = emfp.getEntityManager();
      if (em != null) {// 如果对象已经有em了就不管
        return point.proceed();
      } else {
        em = entityManagerFactory.createEntityManager();
      }
      if (isTransaction) {
        EntityTransaction t = null;
        try {
// 打开连接并开启事务
          t = em.getTransaction();
          if (!t.isActive())
            t.begin();
          emfp.setEntityManager(em);
          Object obj = point.proceed();
// 提交事务
          t.commit();
          return obj;
        } catch (Exception e) {
          if (t != null) {

            t.rollback();
          }
          e.printStackTrace();
          throw e;
        } finally {
          if (em != null && em.isOpen()) {// 关闭连接
            em.close();
          }
          emfp.setEntityManager(null);
        }
      } else {
        try {
          emfp.setEntityManager(em);
          return point.proceed();
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        } finally {
          if (em != null && em.isOpen()) {// 关闭连接
            em.close();
          }
          emfp.setEntityManager(null);
        }
      }
    } else {
      return point.proceed();
    }
  }

  public String[] getTxmethod() {
    return txmethod;
  }

  public void setTxmethod(String[] txmethod) {
    this.txmethod = txmethod;
  }

  public void setEntityManagerFactory(
    EntityManagerFactory entityManagerFactory) {
    this.entityManagerFactory = entityManagerFactory;
  }
}