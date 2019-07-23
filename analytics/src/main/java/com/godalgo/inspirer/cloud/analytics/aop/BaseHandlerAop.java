package com.godalgo.inspirer.cloud.analytics.aop;

import com.godalgo.inspirer.cloud.analytics.handler.BaseHandler;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 杭盖 on 2018/6/8.
 */
@Component
@Aspect
public class BaseHandlerAop extends BaseAop {

  @Pointcut(value = "execution(public * com.godalgo.inspirer.cloud.analytics.handler.*.*(..)) && !execution(public * com.godalgo.inspirer.cloud.analytics.handler.SearchHandler.*(..))")
  private void parse() {}

  @Around(value = "parse() && args(request,nativeSql,pageable)")
  public Object query(ProceedingJoinPoint joinPoint, HttpServletRequest request, String nativeSql, Pageable pageable) throws Throwable {
    System.out.println("Before JoinPoint for " + joinPoint.toString());
    String joinPointName = joinPoint.getSignature().getName();
    Method method = joinPoint.getTarget().getClass().getDeclaredMethod(joinPointName, HttpServletRequest.class, String.class, Pageable.class);
    Object result = executeNativeSql(joinPoint, request, nativeSql, method);
    return result;
  }

  @Around(value = "parse() && args(request,nativeSql)")
  public Object upsert(ProceedingJoinPoint joinPoint, HttpServletRequest request, String nativeSql) throws Throwable {
    System.out.println("Before JoinPoint for " + joinPoint.toString());
    String joinPointName = joinPoint.getSignature().getName();
    Method method = joinPoint.getTarget().getClass().getDeclaredMethod(joinPointName, HttpServletRequest.class, String.class);
    Object result = executeNativeSql(joinPoint, request, nativeSql, method);
    return result;
  }
}
