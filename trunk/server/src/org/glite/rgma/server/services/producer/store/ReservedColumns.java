/*
 * Copyright (c) Members of the EGEE Collaboration. 2004.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.producer.store;

import org.glite.rgma.server.services.sql.ColumnDefinition;
import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.DataType;
import org.glite.rgma.server.services.sql.ExpressionOrConstant;
import org.glite.rgma.server.services.sql.DataType.Type;

/**
 * Column definitions used by R-GMA.
 */
public class ReservedColumns {
	/* RgmaTUID */
	public static final ColumnDefinition RGMA_TUID_COLUMN = new ColumnDefinition();

	public static final String RGMA_TUID_COLUMN_NAME = "RgmaTUID";

	public static final ExpressionOrConstant RGMA_TUID_COLUMN_CONSTANT = new Constant(RGMA_TUID_COLUMN_NAME, Constant.Type.COLUMN_NAME);

	/* TUIDOneOff */
	public static final ColumnDefinition RGMA_TUID_ONE_OFF_COLUMN = new ColumnDefinition();

	public static final String RGMA_TUID_ONE_OFF_COLUMN_NAME = "RgmaOneOffTUID";

	/* RgmaInsertTime */
	public static final ColumnDefinition RGMA_INSERT_TIME_COLUMN = new ColumnDefinition();

	public static final String RGMA_INSERT_TIME_COLUMN_NAME = "RgmaInsertTime";

	/* RgmaTimeStamp */
	public static final String RGMA_TIMESTAMP_COLUMN_NAME = "RgmaTimestamp";

	public static final ExpressionOrConstant RGMA_TIMESTAMP_COLUMN_CONSTANT = new Constant(RGMA_TIMESTAMP_COLUMN_NAME, Constant.Type.COLUMN_NAME);

	/* RgmaLRT column added to all tables. */
	public static final String RGMA_LRT_COLUMN_NAME = "RgmaLRT";

	/* Server where the tuple was first inserted */
	public static final String RGMA_ORIGINAL_SERVER = "RgmaOriginalServer";

	/* Client where the tuple was first inserted */
	public static final String RGMA_ORIGINAL_CLIENT = "RgmaOriginalClient";

	static {
		RGMA_TUID_COLUMN.setName(RGMA_TUID_COLUMN_NAME);
		RGMA_TUID_COLUMN.setType(new DataType(Type.INTEGER, 0));

		RGMA_TUID_ONE_OFF_COLUMN.setName(RGMA_TUID_ONE_OFF_COLUMN_NAME);
		RGMA_TUID_ONE_OFF_COLUMN.setType(new DataType(Type.INTEGER, 0));

		RGMA_INSERT_TIME_COLUMN.setName(RGMA_INSERT_TIME_COLUMN_NAME);
		RGMA_INSERT_TIME_COLUMN.setType(new DataType(Type.TIMESTAMP, 0));
		RGMA_INSERT_TIME_COLUMN.setNotNull(true);
	}

	private ReservedColumns() {
	}
}
