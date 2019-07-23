package com.godalgo.inspirer.cloud.analytics.configuration;

import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Created by 杭盖 on 2018/1/3.
 */
@Configuration
@Profile("cloud")
public class CloudFoundryConfiguration {
  @Bean
  public Cloud cloud() {
    return new CloudFactory().getCloud();
  }
}
