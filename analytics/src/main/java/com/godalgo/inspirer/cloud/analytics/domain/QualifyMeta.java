package com.godalgo.inspirer.cloud.analytics.domain;

import com.godalgo.inspirer.cloud.analytics.annoation.FieldMap;
import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;
/*
belongs_to
has_one
has_many
has_many :through
has_one :through
has_and_belongs_to_many
 */

/**
 * Created by 杭盖 on 2018/3/19.
 */
@Entity
@Data
@Table(name = "qualify_metas")
public class QualifyMeta extends BaseDomain<QualifyMeta> {
  @Id
  Long id;

  private String domain;
  private String table;

  @Column(name="FAMILY", columnDefinition="VARCHAR(16) NOT NULL")
  private String family;

  private String name;


}
