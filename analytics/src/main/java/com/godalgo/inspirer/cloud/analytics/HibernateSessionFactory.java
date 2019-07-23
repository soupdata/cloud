package com.godalgo.inspirer.cloud.analytics;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.util.Properties;

/**
 * Created by 杭盖 on 2018/9/3.
 */
public class HibernateSessionFactory {
  private static final ThreadLocal<Session> threadLocal = new ThreadLocal(); // 本地线程
  private static SessionFactory sessionFactory; // 声明一个SessionFactory对象

  private static SessionFactory buildSessionFactory() {
    sessionFactory = new Configuration().configure().buildSessionFactory();
    return sessionFactory;
  }

  public static SessionFactory getSessionFactory() {
    return (sessionFactory == null ? buildSessionFactory() : sessionFactory);
  }

  public static Session openSession() {
    return getSessionFactory().openSession();
  }

  public static Session getSession() throws HibernateException {
    Session session = threadLocal.get();
    // 如果这个链接没开
    if (session == null || !session.isOpen()) {
      if (sessionFactory == null) {
        buildSessionFactory();
      }
      // 是否开放一个session
      session = sessionFactory.openSession();
      // 把session放入本地线程
      threadLocal.set(session);
    }
    return session;
  }

  // 关闭连接
  public static void closeSession() throws HibernateException {
    Session session = threadLocal.get();
    threadLocal.remove();
    if (session != null) {
      session.close();
    }

  }
}
