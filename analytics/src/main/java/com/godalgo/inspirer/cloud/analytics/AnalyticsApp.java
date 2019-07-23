/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.godalgo.inspirer.cloud.analytics;

import com.godalgo.inspirer.cloud.analytics.domain.Event;
import com.godalgo.inspirer.cloud.analytics.repository.support.CustomRepositoryFactoryBean;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.zuul.EnableZuulProxy;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.*;

import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;

import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.rest.core.config.RepositoryRestConfiguration;
import org.springframework.data.rest.webmvc.config.RepositoryRestConfigurerAdapter;


import org.springframework.transaction.annotation.EnableTransactionManagement;


import javax.annotation.PostConstruct;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author 杭盖
 */
// 使用Spring Boot框架也能使用XML配置, 只要在程序入口使用一个注解即@ImportResource({"classpath:spring-datasource.xml"}),
// 但是Spring Boot更推荐使用application.properties或application.yml文件来进行配置
// @EnableTransactionManagement 默认自动开启
// Spring Boot 自动开启了对 Spring Data JPA 的支持，即我们无须在配置类中显示声明 @EnableJpaRepositories
//@Slf4j
@PropertySource(value = {"classpath:/application.yml"},
  factory = AnalyticsApp.MapPropertySourceFactory.class)
@EnableZuulProxy
@EnableCircuitBreaker
@EnableDiscoveryClient
@EnableAspectJAutoProxy(exposeProxy = true)
// TODO: AdviceMode.ASPECTJ会导致报javax.persistence.TransactionRequiredException错误
@EnableTransactionManagement(mode = AdviceMode.PROXY)
@EnableJpaRepositories(
  basePackages = {"com.godalgo.inspirer.cloud.analytics.repository"},
  repositoryFactoryBeanClass = CustomRepositoryFactoryBean.class)
@SpringBootApplication
public class AnalyticsApp extends RepositoryRestConfigurerAdapter implements ApplicationContextAware {
  private static ApplicationContext applicationContext;
  @Autowired
  private Environment env;

  // ApplicationContext ctx = new FileSystemXmlApplicationContext("applicationContext.xml");
  // ctm = new ClassPathXmlApplicationContext("meta-inf/beans.xml");
  @Override
  public void configureRepositoryRestConfiguration(RepositoryRestConfiguration config) {
    config.exposeIdsFor(Event.class);
  }

  @PostConstruct
  public void exposeIds() {
  }

//  @Autowired
//  EntityManagerFactory entityManagerFactory;
//
//  @Bean
//  public PlatformTransactionManager transactionManager() {
//    JpaTransactionManager transactionManager = new JpaTransactionManager();
//    transactionManager.setEntityManagerFactory(entityManagerFactory);
//
//    return transactionManager;
//  }


  // @Value 无法从多个配置文件获取值
  static final class MapPropertySourceFactory implements PropertySourceFactory {
    @Override
    public org.springframework.core.env.PropertySource<?> createPropertySource(String name, EncodedResource resource) throws IOException {
      if (name == null) name = "applicationConfig:[classpath:/application.yml]";
      YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
      yaml.setResources(resource.getResource());
      Properties properties = yaml.getObject();
      Map<String, Object> map = new HashMap();
      for (Map.Entry<Object, Object> entry : properties.entrySet())
        map.put((String) entry.getKey(), entry.getValue());

      return new MapPropertySource(name, map);
    }

  }

/*
  @Bean
  // 抑制 @Value 中占位符不匹配错误
  public static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
    PropertySourcesPlaceholderConfigurer c = new PropertySourcesPlaceholderConfigurer();
    c.setIgnoreUnresolvablePlaceholders(true);
    return c;
  }
*/

  /*
  @Bean
  // 手动创建
  public EntityManagerFactory entityManagerFactory(DataSource dataSource) throws FileNotFoundException {
    LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
    if (emf.getObject() == null) {
      emf.setPackagesToScan("com.godalgo.inspirer.cloud.analytics.domain");

      dataSource.setUsername("");
      // 密码不能为空
      dataSource.setPassword("");
      dataSource.setUrl(env.getProperty("phoenix.url"));
      dataSource.setDriverClassName(env.getProperty("phoenix.driver"));

      // UDF
      Properties props = new Properties();
      props.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
      props.setProperty("fs.hdfs.impl", env.getProperty("phoenix.fs.hdfs.impl"));
      props.setProperty("hbase.rootdir", env.getProperty("phoenix.hbase.rootdir"));
      props.setProperty("hbase.dynamic.jars.dir", env.getProperty("phoenix.hbase.dynamic.jars.dir"));
      dataSource.setConnectionProperties(props.toString().replace(",", ";"));

      //添加XML目录
      // ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

      Yaml yaml = new Yaml();
      URL url = AnalyticsApp.class.getClassLoader().getResource("application.yml");
      HashMap map = yaml.load(new FileInputStream(url.getFile()));
      HashMap springMap = (HashMap) map.get("spring");
      HashMap jpaMap = (HashMap) springMap.get("jpa");

      HibernateJpaVendorAdapter adapter = new HibernateJpaVendorAdapter();
      adapter.setDatabase(Database.MYSQL);
      adapter.setDatabasePlatform("org.hibernate.dialect.MySQLDialect");
      adapter.setShowSql(true);
      adapter.setGenerateDdl(false);

      emf.setJpaPropertyMap(jpaMap);
      emf.setJpaVendorAdapter(adapter);
      emf.setDataSource(dataSource);
      emf.afterPropertiesSet();
    }
    return emf.getObject();
  }*/


  public static void main(String[] args) {
    SpringApplication.run(AnalyticsApp.class, args);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    AnalyticsApp.applicationContext = applicationContext; // NOSONAR
  }

  public static ApplicationContext getApplicationContext() {
    checkApplicationContext();
    return applicationContext;
  }

  @SuppressWarnings("unchecked")
  public static <T> T getBean(String name) {
    checkApplicationContext();
    return (T) applicationContext.getBean(name);
  }

  @SuppressWarnings("unchecked")
  public static <T> Map<String, T> getBean(Class<T> clazz) {
    checkApplicationContext();
    return applicationContext.getBeansOfType(clazz);
  }

  public static void cleanApplicationContext() {
    applicationContext = null;
  }

  private static void checkApplicationContext() {
    if (applicationContext == null) {
      throw new IllegalStateException("applicaitonContext 未注入, 请在 applicationContext.xml 中定义 SpringContextHolder");
    }
  }
}


