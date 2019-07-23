package com.godalgo.inspirer.cloud.analytics.repository.spec;

import org.springframework.data.jpa.domain.Specification;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;
import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterables.toArray;

/**
 * Created by 杭盖 on 2018/1/5.
 */
public class CustomSpecs {
  public static <T> Specification<T> byAuto(final EntityManager entityManager, final T example) {

    final Class<T> type = (Class<T>) example.getClass();

    return new Specification<T>() {

      @Override
      public Predicate toPredicate(Root<T> root, CriteriaQuery<?> query, CriteriaBuilder cb) {
        // 条件组
        List<Predicate> predicates = new ArrayList<>();

        EntityType<T> entity = entityManager.getMetamodel().entity(type);

        for (Attribute<T, ?> attr : entity.getDeclaredAttributes()) {
          Object attrValue = getValue(example, attr);
          if (attrValue != null) {
            if (attr.getJavaType() == String.class) {
              if (!StringUtils.isEmpty(attrValue)) {
                predicates.add(cb.like(
                  // 路径表达式
                  root.get(attribute(entity, attr.getName(), String.class)),
                  pattern((String) attrValue)
                ));
              }
            } else {
              predicates.add(cb.equal(root.get(attribute(entity, attr.getName(), attrValue.getClass())),
                attrValue));
            }
          }

        }
        return predicates.isEmpty() ? cb.conjunction() : cb.and(toArray(predicates, Predicate.class));//11
      }


      private <T> Object getValue(T example, Attribute<T, ?> attr) {
        return ReflectionUtils.getField((Field) attr.getJavaMember(), example);
      }

      private <E, T> SingularAttribute<T, E> attribute(EntityType<T> entity, String fieldName,
                                                       Class<E> fieldClass) {
        return entity.getDeclaredSingularAttribute(fieldName, fieldClass);
      }

    };
  }

  static private String pattern(String str) {
    return "%" + str + "%";
  }
}