/*
package com.godalgo.inspirer.cloud.analytics.configuration;

import com.godalgo.inspirer.cloud.analytics.repository.PhoenixConnector;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.annotation.Resource;

*/
/**
 * Created by 杭盖 on 2018/1/3.
 *//*

@Slf4j
@Configuration
@EnableAutoConfiguration
@Profile("default")
public class LocalConfiguration {
  private static PhoenixConnector connector;
  //核心代码

  public PhoenixConnector getConnector() {
    return connector;
  }

  //由于注解方式无法注入静态成员，所以改为注入到setter方法。
  //再通过赋值实现bean注入静态类
  @Resource(name = "PhoenixConnector")
  public void setConnector(PhoenixConnector connector) {
    LocalConfiguration.connector = connector;
  }
}*/
