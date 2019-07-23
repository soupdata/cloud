package com.godalgo.inspirer.cloud.analytics.repository.support;


import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;

import javax.persistence.EntityManager;
import java.io.Serializable;
import static com.godalgo.inspirer.cloud.analytics.repository.spec.CustomSpecs.*;

/**
 * Created by 杭盖 on 2018/1/5.
 */
public class CustomRepositoryImpl<T, ID extends Serializable>
  extends SimpleJpaRepository<T, ID> implements CustomRepository<T, ID> {
  private final EntityManager entityManager;
  private final Class<T> domainClass;

  public CustomRepositoryImpl(Class<T> domainClass, EntityManager entityManager) {
    super(domainClass, entityManager);
    this.entityManager = entityManager;
    this.domainClass = domainClass;
  }

  public boolean support(String modelType) {
    return domainClass.getName().equals(modelType);
  }

  @Override
  public Page<T> findByAuto(T example, Pageable pageable) {
    return findAll(byAuto(entityManager, example), pageable);
  }

}