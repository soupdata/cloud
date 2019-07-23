package com.godalgo.inspirer.cloud.analytics.repository.support;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.query.Param;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by 杭盖 on 2018/1/5.
 */
@NoRepositoryBean
public interface CustomRepository<T, ID extends Serializable>
  extends JpaRepository<T, ID>, JpaSpecificationExecutor<T> {

  Page<T> findByAuto(T example, Pageable pageable);
}
