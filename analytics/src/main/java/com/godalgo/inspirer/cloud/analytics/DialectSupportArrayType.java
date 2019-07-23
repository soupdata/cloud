/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package com.godalgo.inspirer.cloud.analytics;


import org.hibernate.dialect.MySQL57Dialect;


import java.sql.Types;

/**
 * @author Gail Badner
 */
public class DialectSupportArrayType extends MySQL57Dialect {
	public DialectSupportArrayType() {
		super();
		//org.apache.phoenix.schema.types
		registerColumnType( Types.ARRAY,"varchar($l) array" );
		registerColumnType( Types.ARRAY,"varchar array" );
		registerColumnType( Types.ARRAY,"varchar[]" );
		registerColumnType( Types.ARRAY,"text[]" );
		registerHibernateType( Types.ARRAY, StringArrayType.class.getName() );

	}


}
