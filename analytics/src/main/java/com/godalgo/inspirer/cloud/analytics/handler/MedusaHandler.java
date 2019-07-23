package com.godalgo.inspirer.cloud.analytics.handler;

/**
 * Created by 杭盖 on 2018/1/3.
 */

import com.godalgo.inspirer.cloud.analytics.annoation.GenericDomain;
import com.godalgo.inspirer.cloud.analytics.domain.EventCession;
import com.godalgo.inspirer.cloud.analytics.repository.support.QueryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.util.List;


@RestController(value="medusa")
@RequestMapping("/do/i18n/search")
public class MedusaHandler extends BaseHandler<EventCession> {
  @Autowired
  private QueryImpl doQuery;

  @RequestMapping(value = "/translate", method = RequestMethod.GET)
  @Query(value = "SELECT TEXT FROM DICTIONARIES WHERE LANG=#{param.get('lang')} AND NAME=#{param.get('name')}", nativeQuery = true)
  @GenericDomain(enable = true)
  public @ResponseBody
  List<?> translate(HttpServletRequest request, String nativeSql) throws SQLException {
    return doQuery.queryBySql(nativeSql);
  }

  @RequestMapping(value = "/upsert", method = RequestMethod.GET)
  @Query(value = "UPSERT INTO DICTIONARIES(DOMAIN,NAME,LANG,TEXT) VALUES(#{domainCondition},#{name},#{lang},#{text})", nativeQuery = true)
  @GenericDomain(enable = true)
  public @ResponseBody
  int upsert(HttpServletRequest request, String nativeSql) {
    return doQuery.updateSql(nativeSql);
  }
}
