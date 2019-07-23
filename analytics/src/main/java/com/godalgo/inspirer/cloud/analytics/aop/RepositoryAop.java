package com.godalgo.inspirer.cloud.analytics.aop;

import com.godalgo.inspirer.cloud.analytics.AnalyticsApp;
import com.godalgo.inspirer.cloud.analytics.HibernateSessionFactory;
import com.godalgo.inspirer.cloud.analytics.annoation.*;
import com.godalgo.inspirer.cloud.analytics.handler.BaseHandler;
import com.godalgo.inspirer.cloud.analytics.repository.EventCessionRepository;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.apache.velocity.context.Context;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ReflectionUtils;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.servlet.http.HttpServletRequest;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by 杭盖 on 2018/4/13.
 */
@Component
@Aspect
public class RepositoryAop extends BaseAop {
  @Autowired
  private QueryImpl doQuery;

  private static String splitor = "\\.";
  private String viewTable = "";

  @Pointcut(value = "execution(* com.godalgo.inspirer.cloud.analytics.repository.*Repository.*(..))")
  private void parse() {
  }

  @Around(value = "parse() && args(handler,nativeSql,pageable)")
  public Object around(ProceedingJoinPoint joinPoint, BaseHandler handler, String nativeSql, Pageable pageable) throws Throwable {
    System.out.println("Before JoinPoint for " + joinPoint.toString());
    String joinPointName = joinPoint.getSignature().getName();
    Method method = joinPoint.getTarget().getClass().getDeclaredMethod(joinPointName, BaseHandler.class, String.class, Pageable.class);
    // 约定: Querys中的Query按顺序执行, 最后一个Query映射到nativeSql
    Querys querysAnno = method.getDeclaredAnnotation(Querys.class);
    Query queryAnno = method.getDeclaredAnnotation(Query.class);

    boolean backdate = false;
    Backdate backdateAnno = method.getDeclaredAnnotation(Backdate.class);
    if (backdateAnno != null) {
      backdate = backdateAnno.enable();
    }

    HashMap<String, String> sqlParams = handler.getSqlParams();
    List<String> unionWhere = handler.getUnionWhere();
    String having = sqlParams.getOrDefault("having", "");

    // 增量添加DynamicColumns
    AddDynamicColumns addDynamicColumnsAnno = method.getDeclaredAnnotation(AddDynamicColumns.class);
    AddDynamicColumn[] dynamicColumns = null;
    if (null != addDynamicColumnsAnno) {
      dynamicColumns = addDynamicColumnsAnno.value();
    } else {
      AddDynamicColumn addDynamicColumnAnno = method.getDeclaredAnnotation(AddDynamicColumn.class);
      if (null != addDynamicColumnAnno)
        dynamicColumns = new AddDynamicColumn[]{addDynamicColumnAnno};
    }
    if (dynamicColumns != null) {
      for (AddDynamicColumn dynamicColumn : dynamicColumns) {
        String column = dynamicColumn.column();
        String type = dynamicColumn.type();
        String[] segments = column.split(splitor);
        String table = segments[0];
        String family, qualify = "";
        List<String> keys = new ArrayList();
        if (segments.length == 3) {
          family = segments[1];
          qualify = segments[2];
        } else {
          family = "";
          qualify = segments[1];
        }
        keys.add(family);
        keys.add(qualify);
        handler.addDynamicColumn(table, String.join(".", keys), type);
      }
    }

    // 从中间表获取需要装填的列信息
    Field[] fields = Class.forName("com.godalgo.inspirer.cloud.analytics.domain.EventCession").getDeclaredFields();
    ArrayList<String> columns = new ArrayList();
    ArrayList<String> columnsWithoutTablePrefix = new ArrayList();
    for (Field field : fields) {
      FieldMap fieldAnno = field.getDeclaredAnnotation(FieldMap.class);
      if (fieldAnno != null) {
        String fieldValue = fieldAnno.field();
        // 元字段需要表前缀
        columns.add(fieldAnno.table() + "." + fieldValue);
        columnsWithoutTablePrefix.add(fieldValue);
      }
    }
    columns.addAll(handler.getAllDynamicColumns(true, false));
    columnsWithoutTablePrefix.addAll(handler.getAllDynamicColumns(false, true));

    String uuid = UUID.randomUUID().toString();

    String upsertSelect = "SELECT " + String.join(",", columns) + ",'" + uuid + "' FROM " + handler.getJoin();
    List<String> unionAll = new ArrayList();

    for (String where : unionWhere) {
      if (!backdate) {
        where = " WHERE " + where + handler.addTimeRange(true);
      } else {
        where = " WHERE " + where;
      }
      unionAll.add(upsertSelect + where);
    }

    System.out.println("Start filling EVENTS_CESSIONS");
    String prepareSql = "UPSERT INTO EVENTS_CESSIONS" +
      "(" + String.join(",", columnsWithoutTablePrefix) + ",UUID) " +
      String.join(" UNION ALL ", unionAll);

    int count = (int) doTransaction(prepareSql, true);
    System.out.println(String.format("%d rows affected", count));

    // 在装载中间表期间，标识为只读，提高性能
    // 共享spring提供的connection
//    Connection connection = doQuery.getEntityManager().unwrap(SessionImplementor.class).connection();
//    boolean isAutoCommit = connection.getAutoCommit();
//    // 关闭自动提交
//    connection.setAutoCommit(false);
//    PreparedStatement statement = connection.prepareStatement(prepareSql);
//    statement.executeUpdate();
//
//    //resultSet.last(); // 将光标移动到最后一行
//    //int rowCount = resultSet.getRow(); // 得到当前行号，即结果集记录数
//    //resultSet.first(); // 还原光标位置
//    // 每页100条数据
//    //int pageSize = 100;
//    //int pageTotal = (int) Math.ceil(rowCount / pageSize);
//    //int pageNo = 0;
//
////    RowSetFactory factory = RowSetProvider.newFactory();
////    CachedRowSet cachedRowSet = factory.createCachedRowSet();
////    cachedRowSet.setPageSize(100);
////    cachedRowSet.populate(resultSet, 1);
//////    while (pageNo++ < pageTotal) {
//////      cachedRowSet.populate(resultSet, (pageNo - 1) * pageSize + 1);
//////    }
////    while (cachedRowSet.nextPage()) {
////    }
//
//    statement.close();
//    // 还原
//    connection.setAutoCommit(isAutoCommit);

    // 事务支持
//    EntityManager entityManager = doQuery.getEntityManager();
//    EntityTransaction tx = entityManager.getTransaction();
//    try {
//      if (!tx.isActive()) {
//        tx.begin();
//      }
//      doQuery.updateSql("CREATE VIEW IF NOT EXISTS DERIVED_VIEW AS SELECT * FROM events");
//      tx.commit();
//    } catch (Exception e) {
//      if (tx != null) {
//        tx.rollback();
//      }
//      throw e;
//    } finally {
//      if (entityManager.isOpen()) {
//        // 总是保持一个连接？
////      entityManager.close();
//      }
////      doQuery.setEntityManager(null);
//    }
    Context context = getContext(handler.getRequest(), method);
    context.put("uuid", uuid);
    context.put("utime", handler.getRequest().getParameter("utime"));
    // groupBy, having子句在这里都是为中间表服务的，去掉原始表前缀
    context.put("group", handler.stripTablePrefix(",", handler.getRawGroupList()));
    context.put("having", "HAVING " + handler.getCondition(handler.getRawHaving(), true));
    // 暂时不支持
//    context.put("having", stripTablePrefix(" HAVING ", handler.getRawHavingList()));

    ContextFactoryMethod contextFactoryMethodAnno = method.getDeclaredAnnotation(ContextFactoryMethod.class);
    if (contextFactoryMethodAnno != null) {
      String methodName = contextFactoryMethodAnno.method();
      Method factory = ReflectionUtils.findMethod(ContextFactory.class, methodName, new Class[]{BaseHandler.class, Context.class, String.class});
      if (null != factory) {
        ReflectionUtils.invokeMethod(factory, ContextFactory.class, handler, context, contextFactoryMethodAnno.params());
      }

    }

    Object[] args = joinPoint.getArgs();

    // 通过having子句限制数据输出区间
    if (!having.isEmpty() && backdate) {
      having = " HAVING " + having + handler.addTimeRange(true);
    } else if (having.isEmpty() && backdate) {
      having = " HAVING " + handler.addTimeRange(false);
    } else if (!having.isEmpty() && !backdate) {
      having = " HAVING " + having;
    }

    if (querysAnno != null) {
      Query[] querys = querysAnno.value();
      for (int i = 0; i < querys.length; i++) {
        String finalHaving = having;
        parseQuery(querys[i], (i == querys.length - 1), context, (queryWriter) -> {
          args[1] = ((StringWriter) queryWriter).getBuffer().toString() + finalHaving;
        });
      }
    } else {
      String finalHaving = having;
      parseQuery(queryAnno, true, context, (nativeQuerySql) -> {
        args[1] = nativeQuerySql + finalHaving;
      });
    }

    Object result = joinPoint.proceed(args);

    System.out.println("After JoinPoint for " + joinPoint.toString());

    // 中间表利用完成后, 删除此次批次数据
/*    List<HashMap> rr = doQuery.queryBySql("SELECT COUNT(*) C FROM EVENTS_CESSIONS");
    System.out.println("Fucking.......");
    System.out.println(rr.get(0).get("C"));*/
    System.out.println("Truncate table events_cessions of " + uuid);
    count = (int) doTransaction("DELETE FROM EVENTS_CESSIONS WHERE UUID='" + uuid + "'", true);
    System.out.println(String.format("%d rows affected", count));

    // 约定: name为中间表的名称
    if (!viewTable.isEmpty()) {
      System.out.println("Truncate table " + viewTable + " of " + uuid);
      count = (int) doTransaction("DELETE FROM " + viewTable + " WHERE UUID='" + uuid + "'", true);
      System.out.println(String.format("%d rows affected", count));
    }


    return result;
  }

  private void parseQuery(Query queryAnno, Boolean isLast, Context context, Consumer<Object> consumer) {
    String query = queryAnno.value();
    String countQuery = queryAnno.countQuery();
    StringWriter queryWriter = getStringWriter(context, query);

    String nativeQuerySql = queryWriter.getBuffer().toString();

    // 约定: name与countName不能同时出现
    String tableName = queryAnno.name();
    String countName = queryAnno.countName();
    if (!tableName.isEmpty()) {
      viewTable = tableName;
      // 约定: 若提供count query, 则query用作装填中间表
      System.out.println("Start filling " + tableName);
      int count = (int) doTransaction(nativeQuerySql, true);
      System.out.println(String.format("%d rows affected", count));
    } else if (!countName.isEmpty()) {
      // 约定: 在上下文中, countName作为前一个SQL执行结果的key
      List<HashMap> rs = doQuery.queryBySql(nativeQuerySql);
      HashMap row = rs.get(0);
      context.put(countName, row);
    }
    // 约定: 有countQuery就一定会有value
    if (!countQuery.isEmpty()) {
      StringWriter countQueryWriter = getStringWriter(context, countQuery);
      if (isLast) consumer.accept(countQueryWriter);
      else doQuery.queryBySql(countQueryWriter.getBuffer().toString());
    } else {
      if (isLast) consumer.accept(nativeQuerySql);
    }
  }

  public Object doTransaction(String nativeSql, Boolean update) {
    Object rtr = null;

    Session session = HibernateSessionFactory.getSession();
    if (!update) {
      // 无论是Load还是Get都会首先查找缓存一级缓存, 如果没有, 才会去数据库查找, 调用Clear()方法, 可以强制清除Session缓存
      session.clear();
      rtr = doQuery.queryBySql(nativeSql, session);
    } else {
      // 更新需要事务
      Transaction tx = session.beginTransaction();
      try {
        System.out.println("-------------------"+nativeSql);
        rtr = doQuery.updateSql(nativeSql, session);
        //session.flush();
        tx.commit();
      } catch (Exception ex) {
        ex.printStackTrace();

        System.out.println("Start rollback...");
        tx.rollback();
      } finally {
        // 手动关闭session
        HibernateSessionFactory.closeSession();
      }

    }
    return rtr;
  }

}
