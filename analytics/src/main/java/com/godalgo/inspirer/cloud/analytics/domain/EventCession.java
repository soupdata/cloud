package com.godalgo.inspirer.cloud.analytics.domain;

import com.godalgo.inspirer.cloud.analytics.annoation.AddI18N;
import com.godalgo.inspirer.cloud.analytics.annoation.FamilyMap;
import com.godalgo.inspirer.cloud.analytics.annoation.FieldMap;
import com.godalgo.inspirer.cloud.analytics.annoation.I18N;
import com.vladmihalcea.hibernate.type.json.JsonNodeBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;
import lombok.Data;
import org.hibernate.annotations.Type;

import javax.persistence.*;
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
@Table(name = "events_cessions")
//@SecondaryTables( value = {
//  @SecondaryTable(name = "events", pkJoinColumns = {
//    @PrimaryKeyJoinColumn(name = "domain", referencedColumnName = "domain"),
//    @PrimaryKeyJoinColumn(name = "name", referencedColumnName = "name"),
//    @PrimaryKeyJoinColumn(name = "sid", referencedColumnName = "sid"),
//    @PrimaryKeyJoinColumn(name = "epoch", referencedColumnName = "epoch"),
//    @PrimaryKeyJoinColumn(name = "msec", referencedColumnName = "msec")
//  }),
//  @SecondaryTable(name = "cessions", pkJoinColumns = {
//    @PrimaryKeyJoinColumn(name = "domain", referencedColumnName = "domain"),
//    @PrimaryKeyJoinColumn(name = "name", referencedColumnName = "name"),
//    @PrimaryKeyJoinColumn(name = "msec", referencedColumnName = "epoch")
//  })
//})
public class EventCession extends BaseDomain<EventCession> {
  @Id
  Long id;


  @FieldMap(table = "events", field = "domain")
  private String domain;
  @FieldMap(table = "events", field = "name")
  private String name;
  @FieldMap(table = "events", field = "sid")
  private String sid;
  @FieldMap(table = "events", field = "epoch")
  private Timestamp epoch;
  @FieldMap(table = "events", field = "msec")
  private Timestamp msec;

  @Type(type = "json")
  @Column(columnDefinition = "json")
  private JsonStringType event;

  @Type(type = "json")
  @Column(columnDefinition = "json")
  private JsonStringType element;

  @Type(type = "json")
  @Column(columnDefinition = "json")
  private JsonStringType performance;

  @Type(type = "json")
  @Column(columnDefinition = "json")
  private JsonStringType ajax;

  @Type(type = "json")
  @Column(columnDefinition = "json")
  private JsonStringType thrown;

  @Type(type = "json")
  @Column(columnDefinition = "json")
  private JsonStringType cession;

  @Type(type = "json")
  @Column(columnDefinition = "json")
  private JsonStringType page;
}
