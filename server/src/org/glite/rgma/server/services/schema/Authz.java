package org.glite.rgma.server.services.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.glite.rgma.server.services.sql.ColumnValue;
import org.glite.rgma.server.services.sql.Expression;
import org.glite.rgma.server.services.sql.ExpressionOrConstant;
import org.glite.rgma.server.services.sql.ProducerPredicate;
import org.glite.rgma.server.services.sql.WhereClause;
import org.glite.rgma.server.services.sql.parser.ParseException;
import org.glite.rgma.server.system.RGMAPermanentException;
import org.glite.voms.FQAN;

public class Authz {

	static final Pattern s_dataPattern = Pattern.compile("\\s*(.*?)\\s*:\\s*(.*?)\\s*:\\s*([RW]{1,2})\\s*");

	static final Pattern s_schemaPattern = Pattern.compile("\\s*(.*?)\\s*:\\s*([CRW]{1,3})\\s*");

	public enum RuleType {
		DATA, SCHEMA
	};

	public enum RuleApplicability {
		TABLE, VIEW
	};

	/**
	 * Constructs a WHERE predicate from applicable table authorization rules. TableNames may be null.
	 */
	public static Expression constructAuthPredicate(List<String> tableNames, String clientDN, List<FQAN> fqans, List<String> tableAuthorization,
			RuleType ruleType, char requestedAction) throws RGMAPermanentException {
		List<ExpressionOrConstant> authorizationRuleExpressions = new ArrayList<ExpressionOrConstant>();
		Expression authorizationPredicate;

		if (fqans.size() == 0) {
			fqans = new ArrayList<FQAN>(1);
			fqans.add(new FQAN("", "", ""));
		}
		/**
		 * Iterate through the rules and build up the authorization predicate. It will be in the form: WHERE
		 * ((rulePredicate[FQAN[0]] AND ruleCredential[FQAN[0]]) OR (rulePredicate[FQAN[1]] AND ruleCredential[FQAN[1]])
		 * ...)
		 */
		try {
			int index = 0;
			for (String rule : tableAuthorization) {
				String predicate;
				String credential;
				String ruleAction;
				if (ruleType == RuleType.DATA) {
					Matcher m = s_dataPattern.matcher(rule);
					if (!m.matches() || m.groupCount() != 3) {
						continue;
					}
					predicate = m.group(1);
					credential = m.group(2);
					ruleAction = m.group(3);
				} else {
					Matcher m = s_schemaPattern.matcher(rule);
					if (!m.matches() || m.groupCount() != 2) {
						continue;
					}
					predicate = "";
					credential = m.group(1);
					ruleAction = m.group(2);
				}
			
				/*
				 * FIXME this removal of the leading / (in group, role and capability looks odd - however it is harmless
				 * so we leave it for now. In the same way it is not clear that the capability and role can ever be
				 * null.
				 */
				for (FQAN f : fqans) {
					String vo = f.getGroup().startsWith("/") ? f.getGroup().substring(1) : f.getGroup();
					String role = f.getRole();
					if (role == null) {
						role = "";
					} else if (role.startsWith("/")) {
						role = role.substring(1);
					}
					String capability = f.getCapability();
					if (capability == null) {
						capability = "";
					} else if (capability.startsWith("/")) {
						capability = capability.substring(1);
					}
					if (ruleAction.indexOf(requestedAction) >= 0) {
						if (!predicate.equals("") && !credential.equals("")) {
							String tempPredicate = predicate;
							if (tableNames != null && tableNames.size() > index) {
								String tableName = tableNames.get(index);
								WhereClause wc = WhereClause.parse(predicate);
								wc.prependTableName(tableName);
								tempPredicate = wc.toString();
							}
							authorizationRuleExpressions.add(getExpression(replacedTokens(tempPredicate, clientDN, vo, role, capability) + "AND ("
									+ replacedTokens(credential, clientDN, vo, role, capability) + ")"));
						} else if (predicate.equals("") && !credential.equals("")) {
							authorizationRuleExpressions.add(getExpression("WHERE " + replacedTokens(credential, clientDN, vo, role, capability)));
						} else if (!predicate.equals("") && credential.equals("")) {
							String tempPredicate = predicate;
							if (tableNames != null && tableNames.size() > index) {
								String tableName = tableNames.get(index);
								WhereClause wc = WhereClause.parse(predicate);
								wc.prependTableName(tableName);
								tempPredicate = wc.toString();
							}

							authorizationRuleExpressions.add(getExpression(replacedTokens(tempPredicate, clientDN, vo, role, capability)));
						} else if (predicate.equals("") && credential.equals("")) {
							authorizationRuleExpressions.add(getExpression("WHERE 1 = 1"));
						}
					}
				}
				index++;
			}

			if (authorizationRuleExpressions.size() == 1) {
				authorizationPredicate = (Expression) authorizationRuleExpressions.get(0);
			} else if (authorizationRuleExpressions.size() > 1) {
				authorizationPredicate = new Expression("OR");
				authorizationPredicate.setOperands(authorizationRuleExpressions);
			} else {
				/* no applicable rules found, so default to deny access */
				authorizationPredicate = getExpression("WHERE 1 = 0");
			}
		} catch (ParseException e) {
			throw new RGMAPermanentException("Error creating authorization predicate: ", e);
		}
		return authorizationPredicate;
	}

	/**
	 * Create an Expression object from a WHERE predicate
	 * 
	 * @param string
	 *            a WHERE SQL predicate
	 * @return Expression an expression object containing the WHERE predicate
	 */
	private static Expression getExpression(String string) throws ParseException {
		return WhereClause.parse(string).getExpression();
	}

	/**
	 * Replaces the credential tokens with values extracted from the clients certificate
	 */
	private static String replacedTokens(String toReplace, String dn, String group, String role, String capability) {
		if (!toReplace.equals("")) {
			toReplace = toReplace.replaceAll("\\[\\bDN\\b\\]", "'" + dn + "'");
			toReplace = toReplace.replaceAll("\\[\\bGROUP\\b\\]", "'" + group + "'");
			toReplace = toReplace.replaceAll("\\[\\bROLE\\b\\]", "'" + role + "'");
			toReplace = toReplace.replaceAll("\\[\\bCAPABILITY\\b\\]", "'" + capability + "'");
		}
		return toReplace;
	}

	static void checkAction(String allowed, String requested) throws RGMAPermanentException {
		for (int i = 0; i < requested.length(); i++) {
			if (allowed.indexOf(requested.charAt(i)) < 0) {
				throw new RGMAPermanentException("Invalid value '" + requested + "' for rule action.");
			}
		}
	}

	static boolean checkRules(List<String> rules, List<String> colNamesUpper, RuleApplicability applicability) throws RGMAPermanentException {
		boolean schemaRuleFound = false;
		for (String rule : rules) {

			String predicate;
			String credential;
			String ruleAction;

			Matcher m = s_dataPattern.matcher(rule);
			if (m.matches() && m.groupCount() == 3) {
				predicate = m.group(1);
				credential = m.group(2);
				ruleAction = m.group(3);
				if (applicability == RuleApplicability.TABLE) {
					checkAction("RW", ruleAction);
				} else {
					checkAction("R", ruleAction);
				}
			} else {
				m = s_schemaPattern.matcher(rule);
				if (m.matches() && m.groupCount() == 2) {
					schemaRuleFound = true;
					predicate = "";
					credential = m.group(1);
					ruleAction = m.group(2);
					if (applicability == RuleApplicability.TABLE) {
						checkAction("RW", ruleAction);
					} else {
						checkAction("", ruleAction);
					}
				} else {
					throw new RGMAPermanentException("Invalid rule format: '" + rule + "'");
				}
			}

			if (!predicate.equals("")) {
				predicate = predicate.replaceAll("\\[\\bDN\\b\\]", "'dn'");
				predicate = predicate.replaceAll("\\[\\bGROUP\\b\\]", "'group'");
				predicate = predicate.replaceAll("\\[\\bROLE\\b\\]", "'role'");
				predicate = predicate.replaceAll("\\[\\bCAPABILITY\\b\\]", "'capability'");
				predicate = predicate.replaceAll("\\bOR\\b", "AND");
				predicate = predicate.replaceAll("\\bNOT\\b", "");
				predicate = predicate.replaceAll("\\bIN\\b", "=");
				predicate = predicate.replaceAll("\\bLIKE\\b", "=");
				predicate = predicate.replaceAll("<>", "=");
				predicate = predicate.replaceAll("<", "=");
				predicate = predicate.replaceAll(">", "=");
				predicate = predicate.replaceAll("\\bIS NULL\\b", "= '123'");
				ProducerPredicate rulePre = null;
				try {
					rulePre = ProducerPredicate.parse(predicate.trim());
				} catch (ParseException e) {
					throw new RGMAPermanentException("Invalid rule format: '" + rule + "'.");
				}
				for (ColumnValue columnValue : rulePre.getColumnValues()) {
					if (!colNamesUpper.contains(columnValue.getName().toUpperCase())) {
						throw new RGMAPermanentException("Rule: '" + rule + "' references non-existent columns.");
					}
				}
			}

			if (!credential.equals("")) {
				credential = credential.replaceAll("\\[\\bDN\\b\\]", "dn");
				credential = credential.replaceAll("\\[\\bGROUP\\b\\]", "gp");
				credential = credential.replaceAll("\\[\\bROLE\\b\\]", "role");
				credential = credential.replaceAll("\\[\\bCAPABILITY\\b\\]", "capability");
				credential = credential.replaceAll("\\bOR\\b", "AND");
				credential = credential.replaceAll("\\bNOT\\b", "");
				try {
					ProducerPredicate.parse(("WHERE " + credential));
					/*
					 * Transform a credential into a predicate in term of syntax (with WHERE clause)
					 */
				} catch (ParseException e) {
					throw new RGMAPermanentException("Invalid rule format: '" + rule + "'.");
				}
			}
		}
		return schemaRuleFound;
	}

}
