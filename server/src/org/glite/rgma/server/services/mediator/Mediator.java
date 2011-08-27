/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2008.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

package org.glite.rgma.server.services.mediator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.glite.rgma.server.services.registry.RegistryService;
import org.glite.rgma.server.services.sql.ColumnValue;
import org.glite.rgma.server.services.sql.Constant;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.ExpressionOrConstant;
import org.glite.rgma.server.services.sql.ProducerPredicate;
import org.glite.rgma.server.services.sql.SelectItem;
import org.glite.rgma.server.services.sql.SelectStatement;
import org.glite.rgma.server.services.sql.TableNameAndAlias;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.ProducerTableEntry;
import org.glite.rgma.server.system.ProducerType;
import org.glite.rgma.server.system.QueryProperties;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.rgma.server.system.RGMATemporaryException;
import org.glite.rgma.server.system.ResourceEndpoint;
import org.glite.rgma.server.system.TimeInterval;
import org.glite.rgma.server.system.Units;

/**
 * Creates and maintains query plans for Consumers. The Mediator sits in between the Consumer and the Registry. It's job
 * is to tell a Consumer which producers to contact to answer their query. It also tells Consumers what action to take
 * when a new producer is added or a producer disappears.
 * <p>
 * The bulk of the code here is devoted to handling the multiple VDBs that may be referenced in the query. The registry
 * does most of the predicate matching, but there is also some code here for determining relevance and completeness of
 * producer predicates.
 */

/*
 * FIXME Currently the mediator may assume that all tables in a query are real tables, not views. It needs to translate
 * view names into the names of the underlying tables if getMatchingProducers etc is to work
 */
public class Mediator {
	/** Warning when a complex query is sent to multiple producers */
	private static final String COMPLEX_QUERY_MULTIPLE_PRODUCERS = "Complex query was sent to multiple producers - results may be incomplete";

	/** Warning when a producer HRP is too short for the query interval */
	private static final String HRP_TOO_SHORT = "One or more producers has an insufficient retention period for the query interval - results may be incomplete";

	/** Logger */
	private static final Logger LOG = Logger.getLogger("rgma.services.mediator");

	/** Maximum number of plans that may be returned */
	private static final int MAX_NUMBER_PLANS = 100;

	/**
	 * Warning when no producers exist that can answer the query (but relevant information may exist in the system)
	 */
	private static final String NO_PRODUCERS = "No suitable producers to answer the query";

	/** Warning when some relevant producers could not be included in the query plan */
	private static final String RELEVANT_PRODUCERS_IGNORED = "Other producers exist that don't match the predicate";

	/** Connection to a registry service */
	private final RegistryService m_registry;

	/**
	 * Constructor
	 * 
	 * @param registry
	 *            Registry service to use for querying VDBs
	 */
	public Mediator(RegistryService registry) {
		m_registry = registry;
	}

	/**
	 * Add a new producer to a continuous consumer's plan. This is called by a consumer which receives notification of a
	 * new relevant producer. It checks if the producer should be incorporated into the consumer's query plan, and if so
	 * returns a list of instructions for how the Consumer should modify it's plan to accommodate it. In most cases this
	 * will just be a single 'add' instruction, or nothing. However it is possible that this could trigger further
	 * Registry requests and more significant plan reconstruction.
	 * 
	 * @param consumer
	 *            Consumer who received the notification.
	 * @param select
	 *            Consumer SELECT query, assumed to be validated.
	 * @param queryProperties
	 *            Continuous Consumer query properties.
	 * @param currentPlan
	 *            Current consumer plan.
	 * @param producerTable
	 *            Details of new producer.
	 * @return List of PlanInstruction objects, possible empty..
	 */
	public List<PlanInstruction> addProducerToPlan(ResourceEndpoint consumer, SelectStatement select, QueryProperties queryProperties, Plan currentPlan,
			ProducerTableEntry producerTable) throws RGMAPermanentException {
		List<PlanInstruction> instructions = new ArrayList<PlanInstruction>();
		if (!queryProperties.isContinuous()) {
			return instructions;
		}
		if (!select.isSimpleQuery()) {
			throw new RGMAPermanentException("Continuous queries must be simple");
		}
		long queryIntervalSec = 0;
		if (queryProperties.hasTimeInterval()) {
			queryIntervalSec = queryProperties.getTimeInterval().getValueAs(Units.SECONDS);
		}
		if (producerIsRelevant(producerTable, select, queryProperties) && !producerTable.getProducerType().isSecondary()
				&& !producerIsCovered(producerTable, currentPlan)) {

			String warning = "";
			if (producerTable.getHistoryRetentionPeriod() < queryIntervalSec) {
				warning = HRP_TOO_SHORT;
			}
			instructions.add(new PlanInstruction(new ProducerDetails(producerTable), PlanInstruction.Type.ADD, select, warning));
			LOG.debug("Added producer " + producerTable + " to plan " + currentPlan);
		} else {
			if (!producerIsRelevant(producerTable, select, queryProperties)) {
				LOG.debug("Producer is not relevant " + producerTable + " not adding to plan " + currentPlan);
			} else if (producerTable.getProducerType().isSecondary()) {
				LOG.debug("Producer is not relevant it is a secondaryProducer " + producerTable + " not adding to plan " + currentPlan);
			} else if (producerIsCovered(producerTable, currentPlan)) {
				LOG.debug("Producer is covered " + producerTable + " not adding to plan " + currentPlan);
			}
		}

		return instructions;
	}

	/**
	 * Close a continuous consumer's plan. This is called when a continuous consumer has finished it's query. It
	 * unregisters the consumer.
	 */
	public void closePlan(ResourceEndpoint consumer, Set<String> vdbs) throws RGMATemporaryException, RGMAPermanentException {
		for (String vdb : vdbs) {
			m_registry.unregisterContinuousConsumer(vdb, true, consumer);
		}
	}

	/**
	 * Get a query plan for a new Consumer query Contacts the registry to obtain details of relevant producers and
	 * constructs one or more query plans which specify which producers to contact to answer the query. Plans may be
	 * returned in any order. For a continuous query this method has the side-effect of registering the Consumer. At
	 * least one plan (possibly empty) will always be returned. Plans will always generate correct answers to the query
	 * but may not be complete. Incomplete plans will have a warning attached.
	 * 
	 * @param consumer
	 *            Consumer requesting the plan. This is needed to register a continuous query. May be <code>null</code>
	 *            for one-time consumers.
	 * @param select
	 *            Consumer SELECT query, assumed to be validated.
	 * @param queryProperties
	 *            Consumer query properties.
	 * @param terminationInterval
	 *            Consumer termination interval (required to register a continuous consumer). May be <code>null</code>
	 *            for one-time consumers.
	 * @param tablesForViews
	 *            Mapping from view names to underlying table names for any views in the query.
	 * @return List of Plan objects.
	 * @throws RGMATemporaryException
	 */
	public List<Plan> getPlansForQuery(ResourceEndpoint consumer, SelectStatement select, QueryProperties queryProperties, TimeInterval terminationInterval,
			Map<String, String> tablesForViews, boolean isSecondary) throws RGMAPermanentException, RGMATemporaryException {
		List<Plan> plans = new ArrayList<Plan>();

		LOG.debug("Getting plans for query: " + select);
		List<Plan> subPlans = getPlansForQueryWithoutVdbUnions(consumer, select, queryProperties, terminationInterval, tablesForViews, isSecondary);
		if (plans.size() == 0) {
			plans = subPlans;
		} else if (plans.size() * subPlans.size() <= MAX_NUMBER_PLANS) {
			// Merge each sub-plan with each existing plan
			if (LOG.isDebugEnabled()) {
				LOG.debug("Merging " + subPlans.size() + " sub-plans with " + plans.size() + " existing plans");
			}
			List<Plan> newPlanList = new ArrayList<Plan>();
			for (Plan subPlan : subPlans) {
				for (Plan existingPlan : plans) {
					Plan newPlan = mergePlans(existingPlan, subPlan);
					newPlanList.add(newPlan);
				}
			}
			plans = newPlanList;
		} else {
			// To avoid exceeding our maximum number of plans,
			// just use the first sub-plan and merge it with each
			// existing plan..
			if (LOG.isDebugEnabled()) {
				LOG.debug("Maximum number of plans reached: merging first sub-plan only with " + plans.size() + " existing plans");
			}
			Plan subPlan = subPlans.get(0);
			List<Plan> newPlanList = new ArrayList<Plan>();
			for (Plan existingPlan : plans) {
				Plan newPlan = mergePlans(existingPlan, subPlan);
				newPlanList.add(newPlan);
			}
			plans = newPlanList;
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Returning plans for query: " + select + "(" + queryProperties + ") : " + plans.size() + " plans returned");
		}
		if (LOG.isDebugEnabled()) {
			for (Plan p : plans) {
				LOG.debug("Plan returned: " + p);
			}
		}
		return plans;
	}

	/**
	 * 'Refresh' an existing plan for a continuous query, and update the consumer's registration. Contacts the registry
	 * to update the consumer registration and check if any new relevant producers have been added. If so, returns
	 * instructions for the consumer to modify it's plan accordingly.
	 * <p>
	 * Note that the disappearence of a producer from the registry will NOT in itself result in a 'remove producer from
	 * plan' instruction - provided the producer would still be part of the plan, it is preserved until the consumer
	 * gets a reliable indication that it has actually died.
	 */
	public List<PlanInstruction> refreshPlan(ResourceEndpoint consumer, SelectStatement select, QueryProperties queryProperties, Plan currentPlan,
			TimeInterval terminationInterval, Map<String, String> tablesForViews, boolean isSecondary) throws RGMAPermanentException, RGMATemporaryException {
		List<PlanInstruction> instructions = new ArrayList<PlanInstruction>();

		if (!queryProperties.isContinuous()) {
			return instructions;
		}

		if (!select.isSimpleQuery()) {
			throw new RGMAPermanentException("Mediator:refreshPlan - Continuous queries must be simple");
		}

		long queryIntervalSec = 0;
		if (queryProperties.hasTimeInterval()) {
			queryIntervalSec = queryProperties.getTimeInterval().getValueAs(Units.SECONDS);
		}

		int terminationIntervalSec = 0;
		if (terminationInterval != null) {
			terminationIntervalSec = (int) terminationInterval.getValueAs(Units.SECONDS);
		}

		List<TableNameAndAlias> tables = select.getTables();
		List<String> tableName = new ArrayList<String>();
		tableName.add(getTableName(tables.get(0), tablesForViews)); // Simple query has only one
		// table

		List<String> tnames = new ArrayList<String>();

		for (String t : tableName) {
			String tname = t;
			if (t.indexOf(".") >= 0) {
				tname = t.substring(t.indexOf(".") + 1);
			}
			tnames.add(tname);
		}

		String predicate = "";
		if (select.getWhere() != null) {
			predicate = select.getWhere().toString();
		}

		boolean planHasPrimaryProducers = hasPrimaryProducers(currentPlan);

		Set<String> vdbs = getVdbs(tables);
		for (String vdb : vdbs) {

			List<ProducerTableEntry> relevantProducers = m_registry.getMatchingProducersForTables(vdb, true, tnames, predicate, queryProperties, isSecondary,
					consumer, terminationIntervalSec);
			for (ProducerTableEntry producerTable : relevantProducers) {
				String warning = "";
				if (planHasPrimaryProducers && producerTable.getProducerType().isContinuous() && !producerTable.getProducerType().isSecondary()
						&& !producerIsCovered(producerTable, currentPlan)) {

					if (producerTable.getHistoryRetentionPeriod() < queryIntervalSec) {
						warning = HRP_TOO_SHORT;
					}
					instructions.add(new PlanInstruction(new ProducerDetails(producerTable), PlanInstruction.Type.ADD, select, warning));
				}
			}
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Refreshed plan for '" + select + "' (" + queryProperties + ") - " + instructions.size() + " instructions returned");
		}

		return instructions;
	}

	/**
	 * Remove a producer from a continuous consumer's plan. This is called by a consumer which receives notification
	 * that a relevant producer no longer exists. It checks if the producer is part of the query plan, and if so returns
	 * a list of instructions for how the Consumer should modify it's plan. In most cases this will just be a single
	 * 'remove' instruction, or nothing. However it is possible that this could trigger further Registry requests and
	 * more significant plan reconstruction.
	 */
	public List<PlanInstruction> removeProducerFromPlan(ResourceEndpoint consumer, SelectStatement select, QueryProperties queryProperties, Plan currentPlan,
			ResourceEndpoint producer, Map<String, String> tablesForViews, boolean isSecondary)

	throws RGMAPermanentException, RGMATemporaryException {
		List<PlanInstruction> instructions = new ArrayList<PlanInstruction>();

		// First check if the producer is in the current plan
		ProducerDetails producerDetails = getProducerFromPlan(producer, currentPlan);
		if (producerDetails != null) {
			if (!producerDetails.getProducerType().isSecondary()) {
				// For a primary producer, just remove it from the plan
				instructions.add(new PlanInstruction(producerDetails, PlanInstruction.Type.REMOVE));
			} else {
				// If a secondary producer has been removed we need to replace it.
				List<PlanInstruction> bestChangeWithoutWarning = null;
				List<PlanInstruction> bestChangeWithWarning = null;
				List<Plan> newPlans = getPlansForQuery(consumer, select, queryProperties, null, tablesForViews, isSecondary);
				for (Plan newPlan : newPlans) {
					// Ignore plans containing the producer we wish to remove
					if (getProducerFromPlan(producer, newPlan) == null) {
						List<PlanInstruction> diff = getDifference(currentPlan, newPlan);
						if (newPlan.hasWarning()) {
							if (bestChangeWithWarning == null || bestChangeWithWarning.size() > diff.size()) {
								bestChangeWithWarning = diff;
							}
						} else {
							if (bestChangeWithoutWarning == null || bestChangeWithoutWarning.size() > diff.size()) {
								bestChangeWithoutWarning = diff;
							}
						}
					}
				}

				// Choose a change of plan without warning if possible.
				if (bestChangeWithoutWarning != null) {
					instructions = bestChangeWithoutWarning;
				} else if (bestChangeWithoutWarning != null) {
					instructions = bestChangeWithoutWarning;
				} else {
					// If no replacement producer exists, remove the broken producer with a warning.
					instructions.add(new PlanInstruction(producerDetails, PlanInstruction.Type.REMOVE, NO_PRODUCERS));
				}
			}
		}

		return instructions;
	}

	enum Ops {
		GT, GE, LT, LE, EQ, NE
	};

	private class SimpleWhere {

		private final String m_col;

		private final Ops m_op;

		private final Constant m_value;

		public SimpleWhere(String col, Ops op, Constant value) {
			m_col = col;
			m_op = op;
			m_value = value;
		}

		@Override
		public String toString() {
			return m_col + " " + m_op + " " + m_value;
		}

	}

	/**
	 * Add fixed columns from an SQL expression.
	 * 
	 * @param exp
	 *            The parsed SQL expression.
	 * @param fixedColumns
	 *            Mapping from column name to column value for columns with fixed values
	 * @param aliases
	 * @throws ComplexPredicateException
	 *             if the predicate is too complex to extract fixed values.
	 */
	private void addFixedColumns(Expression exp, List<SimpleWhere> fixedColumns, Map<String, Constant> aliases) throws ComplexPredicateException {
		if (exp == null) {
			return;
		}
		String sop = exp.getOperator();
		boolean compOp = true;
		Ops swopOp = null;
		Ops op = null;
		if (sop.equals("=")) {
			op = swopOp = Ops.EQ;
		} else if (sop.equals(">")) {
			op = Ops.GT;
			swopOp = Ops.LE;
		} else if (sop.equals("<")) {
			op = Ops.LT;
			swopOp = Ops.GE;
		} else if (sop.equals("<>")) {
			op = swopOp = Ops.NE;
		} else if (sop.equals(">=")) {
			op = Ops.GE;
			swopOp = Ops.LT;
		} else if (sop.equals("<=")) {
			op = Ops.LE;
			swopOp = Ops.GT;
		} else {
			compOp = false;
		}
		if (compOp) {
			ExpressionOrConstant op1 = exp.getOperand(0);
			ExpressionOrConstant op2 = exp.getOperand(1);
			String col = null;
			Constant value = null;
			if (op1 instanceof Constant) {
				Constant c1 = (Constant) op1;
				if (c1.getType() == Constant.Type.COLUMN_NAME) {
					col = c1.getValue().toUpperCase();
				} else if (c1.getType() == Constant.Type.STRING || c1.getType() == Constant.Type.NUMBER) {
					value = c1;
				}
			}
			if (op2 instanceof Constant) {
				Constant c2 = (Constant) op2;
				if (c2.getType() == Constant.Type.COLUMN_NAME) {
					col = c2.getValue().toUpperCase();
					op = swopOp;
				} else if (c2.getType() == Constant.Type.STRING || c2.getType() == Constant.Type.NUMBER) {
					value = c2;
				}
			}
			if (col == null || value == null) {
				throw new ComplexPredicateException("Col and value must be specified: " + col + "," + value);
			} else {
				/* Try substituting from aliases */
				Constant newCol = aliases.get(col);
				if (newCol != null) {
					col = newCol.getValue().toUpperCase();
				}
				fixedColumns.add(new SimpleWhere(col, op, value));
			}
		} else if (exp.getOperator().toUpperCase().equals("AND")) {
			for (ExpressionOrConstant operand : exp.getOperands()) {
				if (operand instanceof Expression) {
					addFixedColumns((Expression) operand, fixedColumns, aliases);
				} else {
					throw new ComplexPredicateException("Operand is not an expression");
				}
			}
		} else {
			throw new ComplexPredicateException("Expression is too complex");
		}
	}

	/**
	 * Add information about producer tables to a collection of producer details. The purpose of this method is to
	 * combine information from multiple calls to the registry into a single collection of producers which includes
	 * details of all the tables they publish in all VDBs. For each producer table, if the producer is already known,
	 * the table details are added to its list. If the producer is unknown a new ProducerDetails object is created for
	 * it and added to the collection.
	 * 
	 * @param allProducers
	 *            Mapping from producer endpoint to ProducerDetails.
	 * @param producers
	 *            List of ProducerTableEntry information from the registry.
	 */
	private void addProducers(Map<ResourceEndpoint, ProducerDetails> allProducers, List<ProducerTableEntry> producers) {
		for (ProducerTableEntry producerTable : producers) {
			ProducerDetails producerDetails = allProducers.get(producerTable.getEndpoint());
			if (producerDetails == null) {
				Map<String, DeclaredTable> tables = new HashMap<String, DeclaredTable>();
				tables.put(producerTable.getVdbTableName(), new DeclaredTable(producerTable.getVdbTableName(), producerTable.getHistoryRetentionPeriod(),
						producerTable.getPredicate()));
				producerDetails = new ProducerDetails(producerTable.getEndpoint(), producerTable.getProducerType(), tables);
				allProducers.put(producerTable.getEndpoint(), producerDetails);
			} else {
				Map<String, DeclaredTable> tables = producerDetails.getTables();
				tables.put(producerTable.getVdbTableName(), new DeclaredTable(producerTable.getVdbTableName(), producerTable.getHistoryRetentionPeriod(),
						producerTable.getPredicate()));
			}
		}
	}

	/**
	 * Count the number of primary producers.
	 * 
	 * @param producers
	 *            Collection of ProducerDetails.
	 * @return Number of producers which are primary producers.
	 */
	private int countPrimaryProducers(Collection<ProducerDetails> producers) {
		int count = 0;
		for (ProducerDetails producer : producers) {
			if (!producer.getProducerType().isSecondary() && !producer.getProducerType().isStatic()) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Get a set of PlanInstructions to turn one plan into another.
	 * 
	 * @param currentPlan
	 *            The query plan we want to change from.
	 * @param newPlan
	 *            The query plan we want to change into.
	 * @return A List of PlanInstruction objects.
	 */
	private List<PlanInstruction> getDifference(Plan currentPlan, Plan newPlan) throws RGMAPermanentException {
		List<PlanInstruction> instructions = new ArrayList<PlanInstruction>();

		List<PlanEntry> currentEntries = currentPlan.getPlanEntries();
		List<PlanEntry> newEntries = newPlan.getPlanEntries();

		for (PlanEntry producer : currentEntries) {
			ProducerDetails producerDetails = getProducerFromPlan(producer.getProducer().getEndpoint(), newPlan);
			if (producerDetails == null) {
				instructions.add(new PlanInstruction(producer.getProducer(), PlanInstruction.Type.REMOVE));
			}
		}

		for (PlanEntry producer : newEntries) {
			ProducerDetails producerDetails = getProducerFromPlan(producer.getProducer().getEndpoint(), currentPlan);
			if (producerDetails == null) {
				instructions.add(new PlanInstruction(producer.getProducer(), PlanInstruction.Type.ADD, producer.getSelect()));
			}
		}

		return instructions;
	}

	/**
	 * Get a query plan for a continuous query. Returns a single plan consisting of all relevant primary producers. The
	 * plan has a warning if one or more of the primary producers has an insufficient history retention period to
	 * respect to query time interval.
	 */
	private List<Plan> getPlansForContinuousQuery(ResourceEndpoint consumer, SelectStatement select, QueryProperties queryProperties,
			TimeInterval terminationInterval, Map<String, String> tablesForViews, boolean isSecondary)

	throws RGMAPermanentException, RGMATemporaryException {
		List<PlanEntry> producers = new ArrayList<PlanEntry>();
		String warning = "";

		long queryIntervalSec = 0;
		if (queryProperties.hasTimeInterval()) {
			queryIntervalSec = queryProperties.getTimeInterval().getValueAs(Units.SECONDS);
		}

		int terminationIntervalSec = 0;
		if (terminationInterval != null) {
			terminationIntervalSec = (int) terminationInterval.getValueAs(Units.SECONDS);
		}

		if (!select.isSimpleQuery()) {
			throw new RGMAPermanentException("Continuous queries must be simple");
		}

		List<TableNameAndAlias> tables = select.getTables();
		List<String> tableName = new ArrayList<String>();
		/* Simple query has only one table */
		tableName.add(getTableName(tables.get(0), tablesForViews));
		String predicate = "";
		if (select.getWhere() != null) {
			predicate = select.getWhere().toString();
		}

		List<String> tnames = new ArrayList<String>();

		for (String t : tableName) {
			String tname = t;
			if (t.indexOf(".") >= 0) {
				tname = t.substring(t.indexOf(".") + 1);
			}
			tnames.add(tname);
		}

		String vdb = tables.get(0).getVdbName();

		/*
		 * FIXME pretend to be a secondary consumer so that we only get back primary producers - we need to deal
		 * properly with SPs as they can now all do continuous queries
		 */
		isSecondary = true;
		List<ProducerTableEntry> relevantProducers = m_registry.getMatchingProducersForTables(vdb, true, tnames, predicate, queryProperties, isSecondary,
				consumer, terminationIntervalSec);
		for (ProducerTableEntry producerTable : relevantProducers) {
			// Add all continuous, relevant primary producers to the plan
			if (producerTable.getProducerType().isContinuous()) {
				producers.add(new PlanEntry(new ProducerDetails(producerTable), select));
				if (producerTable.getHistoryRetentionPeriod() < queryIntervalSec) {
					warning = HRP_TOO_SHORT;
				}
			}
		}

		Plan plan = new Plan(producers, warning);
		List<Plan> plans = new ArrayList<Plan>();
		plans.add(plan);
		return plans;
	}

	/**
	 * Get a list of query plans for a one-time query with no VDB unions.
	 */
	private List<Plan> getPlansForOneTimeQuery(SelectStatement select, QueryProperties queryProperties, Map<String, String> tablesForViews)

	throws RGMAPermanentException, RGMATemporaryException {
		List<TableNameAndAlias> tables = select.getTables();
		Set<String> vdbs = getVdbs(tables);

		long queryIntervalSec = 0;
		if (queryProperties.hasTimeInterval()) {
			queryIntervalSec = queryProperties.getTimeInterval().getValueAs(Units.SECONDS);
		}

		Map<ResourceEndpoint, ProducerDetails> allProducers = new HashMap<ResourceEndpoint, ProducerDetails>();

		for (String vdb : vdbs) {
			String[] vdbTables = getTablesForVdb(vdb, tables, tablesForViews);
			List<String> tbls = Arrays.asList(vdbTables);
			List<String> tnames = new ArrayList<String>();
			for (String t : tbls) {
				String tname = t;
				if (t.indexOf(".") >= 0) {
					tname = t.substring(t.indexOf(".") + 1);
				}
				tnames.add(tname);
			}

			List<ProducerTableEntry> producers = m_registry.getMatchingProducersForTables(vdb, true, tnames, "", queryProperties, false, null, 0);
			addProducers(allProducers, producers);

		}

		List<Plan> plans = new ArrayList<Plan>();
		if (!queryProperties.isStatic()) {
			/*
			 * For latest and history queries, look for complete secondary producers for the query and create a plan for
			 * each one found.
			 */
			List<ProducerDetails> completeProducers = getProducersForQuery(allProducers.values(), queryProperties, tables, select, true);
			if (LOG.isDebugEnabled()) {
				LOG.debug("There are " + completeProducers.size() + " complete producers for non static query");
			}
			for (ProducerDetails producer : completeProducers) {
				PlanEntry planEntry = new PlanEntry(producer, select);
				List<PlanEntry> producers = new ArrayList<PlanEntry>();
				String warning = "";

				producers.add(planEntry);
				if (queryProperties.isHistory() && !hrpIsSufficient(producer, queryIntervalSec)) {
					warning = HRP_TOO_SHORT;
				}

				Plan plan = new Plan(producers, warning);
				plans.add(plan);
			}

			if (plans.size() == 0) {
				List<PlanEntry> producers = new ArrayList<PlanEntry>();
				String warning = "";
				int numPrimaryProducers = countPrimaryProducers(allProducers.values());

				LOG.debug("But there are " + numPrimaryProducers + " primary producers");
				if (numPrimaryProducers > 0) {
					/*
					 * If no complete producers were found but primary producers exist, create a plan consisting of all
					 * suitable primary producers with warnings as appropriate.
					 */
					List<ProducerDetails> primaryProducers = getProducersForQuery(allProducers.values(), queryProperties, tables, select, false);
					String vdbTableName = null;
					if (tables.size() == 1) {
						vdbTableName = tables.get(0).getVdbTableName();
					}
					for (ProducerDetails producer : primaryProducers) {
						if (vdbTableName != null) {
							DeclaredTable dt = producer.getTables().get(vdbTableName);
							Answer ans = predicateSatisfied(dt.getPredicate(), select);
							if (ans == Answer.NO) {
								if (LOG.isDebugEnabled())
									LOG.debug("Rejected " + producer + " as predicate match returns " + ans);
								continue;
							}
						}
						PlanEntry planEntry = new PlanEntry(producer, select);
						producers.add(planEntry);
						if (queryProperties.isHistory() && !hrpIsSufficient(producer, queryIntervalSec)) {
							warning = HRP_TOO_SHORT;
						}
					}
				}
				if (producers.size() > 0) {
					if (producers.size() != numPrimaryProducers) {
						warning = RELEVANT_PRODUCERS_IGNORED;
					} else if (!select.isSimpleQuery()) {
						warning = COMPLEX_QUERY_MULTIPLE_PRODUCERS;
					}
				} else {
					warning = NO_PRODUCERS;
				}
				Plan plan = new Plan(producers, warning);
				plans.add(plan);
			}
		} else {
			LOG.debug("static query");
			// For static queries, put all suitable on-demand producers into a single plan with
			// warnings if there are more than one and the query is complex.
			List<ProducerDetails> onDemandProducers = getProducersForQuery(allProducers.values(), queryProperties, tables, select, false);
			List<PlanEntry> producers = new ArrayList<PlanEntry>();
			String warning = "";
			for (ProducerDetails producer : onDemandProducers) {
				PlanEntry planEntry = new PlanEntry(producer, select);
				producers.add(planEntry);
			}
			if (countPrimaryProducers(allProducers.values()) > 0) {
				warning = RELEVANT_PRODUCERS_IGNORED;
			}
			if (producers.size() > 1 && !select.isSimpleQuery()) {
				warning = COMPLEX_QUERY_MULTIPLE_PRODUCERS;
			}

			Plan plan = new Plan(producers, warning);
			plans.add(plan);
		}

		LOG.debug("returning " + plans.size() + " plans");

		return plans;
	}

	/**
	 * Get a query plan for a new Consumer query which does not contain any VDB unions. VDB unions are specified by the
	 * special {} syntax. They are expanded out in getPlansForQuery. TODO get rid of vdb unions from mediator
	 */
	private List<Plan> getPlansForQueryWithoutVdbUnions(ResourceEndpoint consumer, SelectStatement select, QueryProperties queryProperties,
			TimeInterval terminationInterval, Map<String, String> tablesForViews, boolean isSecondary) throws RGMAPermanentException, RGMATemporaryException {
		if (queryProperties.isContinuous()) {
			return getPlansForContinuousQuery(consumer, select, queryProperties, terminationInterval, tablesForViews, isSecondary);
		} else {
			return getPlansForOneTimeQuery(select, queryProperties, tablesForViews);
		}
	}

	/**
	 * Check if a producer is included in a plan Checks if a producer with a specified endpoint is included in the plan.
	 * 
	 * @param producer
	 *            Endpoint of producer to check.
	 * @param plan
	 *            Query plan to check.
	 * @return <code>true</code> if the producer is in the plan.
	 */
	private ProducerDetails getProducerFromPlan(ResourceEndpoint producer, Plan plan) {
		List<PlanEntry> entries = plan.getPlanEntries();

		for (PlanEntry entry : entries) {
			if (entry.getProducer().getEndpoint().equals(producer)) {
				return entry.getProducer();
			}
		}
		return null;
	}

	/**
	 * Get a list of all producers which can answer a query. This means all producers which publish all VDB-tables
	 * referenced in the query and which support the query type.
	 * 
	 * @param allProducers
	 *            Mapping from VDB name to list of matching producer tables from the registry.
	 * @param queryProperties
	 *            Query properties.
	 * @param tables
	 *            List of vdb-tables referenced in the query.
	 * @param select
	 *            Parsed SQL SELECT statement.
	 * @param complete
	 *            If <code>true</code> return only complete secondary producers. If <code>false</code> do NOT return
	 *            secondary producers at all.
	 * @throws RGMAPermanentException
	 */
	private List<ProducerDetails> getProducersForQuery(Collection<ProducerDetails> allProducers, QueryProperties queryProperties,
			List<TableNameAndAlias> tables, SelectStatement select, boolean complete) throws RGMAPermanentException {
		List<ProducerDetails> producersForQuery = new ArrayList<ProducerDetails>();
		for (ProducerDetails producer : allProducers) {
			String failure = null;
			if (!supportsQueryType(producer, queryProperties)) {
				failure = "query type not supported";
			} else if (!publishesAllTables(producer, tables)) {
				failure = "not all tables published";
			} else if (complete) {
				if (!isComplete(producer, select, tables)) {
					failure = "not complete";
				}
			} else {
				if (producer.getProducerType().isSecondary()) {
					failure = "producer is secondary";
				}
			}
			if (failure == null) {
				producersForQuery.add(producer);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Accepted " + producer);
				}
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Rejected " + producer + " - as " + failure);
				}
			}
		}
		return producersForQuery;
	}

	/**
	 * Get the real name of a table referenced in a select query. The real name is the name of the table if it is a real
	 * table, or the name of the underlying table if it is a view.
	 */
	private String getTableName(TableNameAndAlias table, Map<String, String> tablesForViews) {
		String vdbTableName = table.getVdbTableName();
		String underlyingTable = tablesForViews.get(vdbTableName);
		if (underlyingTable == null) {
			return vdbTableName;
		} else {
			return underlyingTable;
		}
	}

	/**
	 * Get the names of tables from a particular VDB.
	 * 
	 * @param vdb
	 *            VDB name.
	 * @param tables
	 *            List of AliasedName objects for tables.
	 * @return Array of table names for this VDB.
	 */
	private String[] getTablesForVdb(String vdb, List<TableNameAndAlias> tables, Map<String, String> tablesForViews) {
		Set<String> vdbTables = new HashSet<String>();
		for (TableNameAndAlias table : tables) {
			String schema = table.getVdbName();
			if (schema.equals(vdb)) {
				vdbTables.add(getTableName(table, tablesForViews));
			}
		}
		return vdbTables.toArray(new String[] {});
	}

	/**
	 * Get the names of all VDBs referenced in a list of tables
	 * 
	 * @param tables
	 *            List of AliasedName objects for tables.
	 * @return Set of VDB names.
	 */
	private Set<String> getVdbs(List<TableNameAndAlias> tables) {
		Set<String> vdbs = new HashSet<String>();
		for (TableNameAndAlias table : tables) {
			vdbs.add(table.getVdbName());
		}
		return vdbs;
	}

	private Answer compatibleWithColumn(List<SimpleWhere> fixedColumns, String column, Constant value) throws RGMAPermanentException {
		SimpleWhere sw = null;
		for (SimpleWhere s : fixedColumns) {
			if (s.m_col.equals(column)) {
				sw = s;
				break;
			}
		}
		if (sw == null) {
			return Answer.MAYBE;
		}
		Answer result;
		if (sw.m_value.getType() != value.getType()) {
			throw new RGMAPermanentException("Predicate does not match query");
		}
		boolean isString = sw.m_value.getType() == Constant.Type.STRING;
		boolean isFloat = sw.m_value.getValue().indexOf('.') >= 0 || value.getValue().indexOf('.') >= 0;
		String compType;
		if (isString) {
			compType = "String";
		} else if (isFloat) {
			compType = "Double";
		} else {
			compType = "Long";
		}
		if (sw.m_op == Ops.EQ) {
			if (isString) {
				result = value.getValue().equals(sw.m_value.getValue()) ? Answer.YES : Answer.NO;
			} else if (isFloat) {
				result = new Double(value.getValue()).compareTo(new Double(sw.m_value.getValue())) == 0 ? Answer.YES : Answer.NO;
			} else {
				result = new Long(value.getValue()).compareTo(new Long(sw.m_value.getValue())) == 0 ? Answer.YES : Answer.NO;
			}
		} else if (sw.m_op == Ops.NE) {
			if (isString) {
				result = !value.getValue().equals(sw.m_value.getValue()) ? Answer.MAYBE : Answer.NO;
			} else if (isFloat) {
				result = new Double(value.getValue()).compareTo(new Double(sw.m_value.getValue())) != 0 ? Answer.MAYBE : Answer.NO;
			} else {
				result = new Long(value.getValue()).compareTo(new Long(sw.m_value.getValue())) != 0 ? Answer.MAYBE : Answer.NO;
			}
		} else if (sw.m_op == Ops.GT) {
			if (isString) {
				result = Answer.MAYBE;
			} else if (isFloat) {
				result = new Double(value.getValue()).compareTo(new Double(sw.m_value.getValue())) > 0 ? Answer.YES : Answer.NO;
			} else {
				result = new Long(value.getValue()).compareTo(new Long(sw.m_value.getValue())) > 0 ? Answer.YES : Answer.NO;
			}
		} else if (sw.m_op == Ops.LT) {
			if (isString) {
				result = Answer.MAYBE;
			} else if (isFloat) {
				result = new Double(value.getValue()).compareTo(new Double(sw.m_value.getValue())) < 0 ? Answer.YES : Answer.NO;
			} else {
				result = new Long(value.getValue()).compareTo(new Long(sw.m_value.getValue())) < 0 ? Answer.YES : Answer.NO;
			}
		} else if (sw.m_op == Ops.GE) {
			if (isString) {
				result = Answer.MAYBE;
			} else if (isFloat) {
				result = new Double(value.getValue()).compareTo(new Double(sw.m_value.getValue())) >= 0 ? Answer.YES : Answer.NO;
			} else {
				result = new Long(value.getValue()).compareTo(new Long(sw.m_value.getValue())) >= 0 ? Answer.YES : Answer.NO;
			}
		} else /* if (sw.m_op == Ops.LE) */{
			if (isString) {
				result = Answer.MAYBE;
			} else if (isFloat) {
				result = new Double(value.getValue()).compareTo(new Double(sw.m_value.getValue())) <= 0 ? Answer.YES : Answer.NO;
			} else {
				result = new Long(value.getValue()).compareTo(new Long(sw.m_value.getValue())) <= 0 ? Answer.YES : Answer.NO;
			}
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Checked " + column + " " + value + " " + sw.m_op + " " + sw.m_value + " as " + compType + " -> " + result);
		}
		return result;
	}

	/**
	 * Check if a plan contains any primary producers
	 * 
	 * @return <code>true</code> if the plan contains at least one primary producer or if it contains no producers at
	 *         all, <code>false</code> otherwise.
	 */
	private boolean hasPrimaryProducers(Plan plan) {
		List<PlanEntry> producers = plan.getPlanEntries();
		if (producers.size() == 0) {
			return true;
		}
		for (PlanEntry entry : plan.getPlanEntries()) {
			if (!entry.getProducer().getProducerType().isSecondary()) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Check if a producer's history retention period is sufficient.
	 * 
	 * @param producer
	 *            Producer details.
	 * @param minHrp
	 *            Minimum history retention period in seconds.
	 * @return <code>true</code> if the HRP for all the producer's tables is greater than minHrp.
	 */
	private boolean hrpIsSufficient(ProducerDetails producer, long minHrp) {
		for (DeclaredTable table : producer.getTables().values()) {
			if (table.getHistoryRetentionPeriod() < minHrp) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Determine if a producer is complete with respect to a query. A producer is complete if all possible results of
	 * the query are published by the producer.
	 * 
	 * @param producer
	 *            Details of the producer from the Registry.
	 * @param select
	 *            Parsed SELECT statement.
	 * @param tableNames
	 *            List of names of tables referenced in the query.
	 * @return <code>true</code> if the producer is complete, <code>false</code> otherwise.
	 * @throws RGMAPermanentException
	 */
	private boolean isComplete(ProducerDetails producer, SelectStatement select, List<TableNameAndAlias> tables) throws RGMAPermanentException {
		if (!producer.getProducerType().isSecondary()) {
			return false;
		} else {
			for (TableNameAndAlias table : tables) {
				DeclaredTable declaredTable = producer.getTables().get(table.getVdbTableName());
				if (declaredTable.getPredicate().trim().length() != 0) {
					if (predicateSatisfied(declaredTable.getPredicate(), select) != Answer.YES) {
						return false;
					}
				}
			}
			return true;
		}
	}

	/**
	 * Merge two plans. Combine two plans into a single plan by merging the producer lists and attaching a warning if
	 * either of the plans has one. If both plans have a warning, use the warning from the first plan.
	 * 
	 * @param plan1
	 *            First plan
	 * @param plan2
	 *            Second plan
	 * @return Merged plan.
	 */
	private Plan mergePlans(Plan plan1, Plan plan2) {
		ArrayList<PlanEntry> entries = new ArrayList<PlanEntry>();
		String warning = "";

		entries.addAll(plan1.getPlanEntries());
		entries.addAll(plan2.getPlanEntries());
		if (plan1.getWarning().length() > 0) {
			warning = plan1.getWarning();
		} else if (plan2.getWarning().length() > 0) {
			warning = plan2.getWarning();
		}

		return new Plan(entries, warning);
	}

	enum Answer {
		YES, NO, MAYBE
	};

	/**
	 * Determine if an SQL predicate is satisfied by a SELECT query. Currently this only works if the predicate and the
	 * query are both simple, i.e. only involve one table with no complex operators such as OR.
	 * 
	 * @param tableName
	 *            Name of the table the query refers to.
	 * @param predicate
	 *            An SQL WHERE predicate.
	 * @param select
	 *            A SELECT statement.
	 * @return <code>true</code> if all results from the query will satisfy the specified predicate
	 * @throws RGMAPermanentException
	 */
	Answer predicateSatisfied(String predicateString, SelectStatement select) throws RGMAPermanentException {
		Answer result = null;
		if (predicateString.trim().length() == 0) {
			result = Answer.YES;
		}
		if (!select.isSimpleQuery()) {
			result = Answer.MAYBE;
		} else {
			try {
				ProducerPredicate predicate = ProducerPredicate.parse(predicateString);
				Map<String, Constant> aliases = new HashMap<String, Constant>();
				for (SelectItem s : select.getSelect()) {
					if (s.getAlias() != null) {
						ExpressionOrConstant esc = s.getExpression();
						if (esc instanceof Constant) {
							aliases.put(s.getAlias().toUpperCase(), (Constant) esc);
						}
					}
				}
				ExpressionOrConstant where = select.getWhere();
				List<SimpleWhere> fixedColumns = new ArrayList<SimpleWhere>();
				addFixedColumns((Expression) where, fixedColumns, aliases);
				result = Answer.YES;
				for (ColumnValue cv : predicate.getColumnValues()) {
					Answer a = compatibleWithColumn(fixedColumns, cv.getName().toUpperCase(), cv.getValue());
					if (a == Answer.MAYBE) {
						result = Answer.MAYBE;
					} else if (a == Answer.NO) {
						result = Answer.NO;
						break;
					}
				}
			} catch (ParseException e) {
				throw new RGMAPermanentException(e);
			} catch (ComplexPredicateException e) {
				result = Answer.MAYBE;
			}
		}
		return result;
	}

	/**
	 * Test if a primary producer is already covered by a query plan.
	 * 
	 * @param producer
	 *            Producer table to check.
	 * @param plan
	 *            Query plan.
	 * @return <code>true</code> if the producer is already included in the plan, either directly or via a Secondary
	 *         Producer.
	 */
	private boolean producerIsCovered(ProducerTableEntry producer, Plan plan) throws RGMAPermanentException {
		if (getProducerFromPlan(producer.getEndpoint(), plan) != null) {
			return true;
		} else {
			String sqlSelect = "SELECT C FROM " + producer.getVdbTableName() + " " + producer.getPredicate();
			try {
				SelectStatement select = SelectStatement.parse(sqlSelect);
				for (PlanEntry entry : plan.getPlanEntries()) {
					if (entry.getProducer().getProducerType().isSecondary() && isComplete(entry.getProducer(), select, select.getTables())) {
						return true;
					}
				}
				return false;
			} catch (ParseException e) {
				throw new RGMAPermanentException("Parsing of autogenerated SQL failed", e);
			}
		}
	}

	/**
	 * Test if a producer table is relevant to a query. Currently just checks if the producer publishes any of the
	 * tables in the query.
	 * 
	 * @param producer
	 *            Producer table entry.
	 * @param select
	 *            Parsed SQL SELECT.
	 * @param queryProperties
	 *            Query properties.
	 * @return <code>true</code> if the producer may have information which answers the query
	 */
	private boolean producerIsRelevant(ProducerTableEntry producer, SelectStatement select, QueryProperties queryProperties) throws RGMAPermanentException {
		String producerVdbTableName = producer.getVdbTableName();
		LOG.debug("trying to match producerVdbTableName " + producerVdbTableName);
		for (TableNameAndAlias table : select.getTables()) {
			LOG.debug("trying to match producerVdbTableName " + producerVdbTableName + " with " + table.getVdbTableName());
			if (table.getVdbTableName().equals(producerVdbTableName)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Determine if a producer publishes all of a list of tables.
	 * 
	 * @param producer
	 *            Producer details.
	 * @param tables
	 *            List of table names.
	 * @return <code>true</code> if the producer publishes all the tables in the list.
	 */
	private boolean publishesAllTables(ProducerDetails producer, List<TableNameAndAlias> tables) {
		boolean publishesAllTables = true;
		for (TableNameAndAlias table : tables) {
			/* Needs fixing for UNIONS */
			if (producer.getTables().get(table.getVdbTableName()) == null) {
				publishesAllTables = false;
				break;
			}
		}
		return publishesAllTables;
	}

	/**
	 * Check if a producer supports a query type
	 * 
	 * @param producer
	 *            Details of producer to check.
	 * @param queryProperties
	 *            Query properties.
	 * @return <code>true</code> if the producer supports the query type defined by queryProperties
	 */
	private boolean supportsQueryType(ProducerDetails producer, QueryProperties queryProperties) {
		ProducerType type = producer.getProducerType();

		if (queryProperties.isContinuous() && type.isContinuous()) {
			return true;
		} else if (queryProperties.isLatest() && type.isLatest()) {
			return true;
		} else if (queryProperties.isHistory() && type.isHistory()) {
			return true;
		} else if (queryProperties.isStatic() && type.isStatic()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Internal exception thrown if a predicate is too complex to determine completeness or relevance
	 */
	@SuppressWarnings("serial")
	private class ComplexPredicateException extends Exception {

		public ComplexPredicateException(String message) {
			super(message);
		}
	}
}
