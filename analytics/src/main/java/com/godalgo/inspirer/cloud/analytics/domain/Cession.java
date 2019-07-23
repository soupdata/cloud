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
package com.godalgo.inspirer.cloud.analytics.domain;

import lombok.Data;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.List;


/**
 * @author Oliver Gierke
 */

@Entity
@Data
@Table(name = "cessions")

public class Cession extends BaseDomain<Cession> {
  @Id
  Long id;

  private String domain;
  private String sid;
  private Timestamp epoch;

  @OneToMany(targetEntity = Event.class)
  @PrimaryKeyJoinColumns({
    @PrimaryKeyJoinColumn(name="domain", referencedColumnName = "domain"),
    @PrimaryKeyJoinColumn(name="sid", referencedColumnName = "sid"),
    @PrimaryKeyJoinColumn(name="epoch", referencedColumnName = "msec")
  })
  public List<Event> events;

}
