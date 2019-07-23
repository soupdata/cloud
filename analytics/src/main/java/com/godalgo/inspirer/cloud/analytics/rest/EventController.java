package com.godalgo.inspirer.cloud.analytics.rest;

import com.godalgo.inspirer.cloud.analytics.repository.EventCessionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by 杭盖 on 2018/1/3.
 */
@RestController
public class EventController {
  @Autowired
  EventCessionRepository repository;

  @RequestMapping("/simple/stores")
  void getStores() {

  }
}