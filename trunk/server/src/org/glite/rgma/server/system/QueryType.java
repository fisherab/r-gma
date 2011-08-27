package org.glite.rgma.server.system;

public class QueryType {
	
	public final static QueryType CONTINUOUS = new QueryType("Continuous");
	public final static QueryType HISTORY = new QueryType("History");
	public final static QueryType LATEST = new QueryType("Latest");
	public final static QueryType STATIC = new QueryType("Static");
	
	private String m_type = null;
	private QueryType(String type){
		m_type = type;
	}
	
	public String toString(){
		return m_type;
	}
	
	public boolean equals(QueryType toTest){
		
		boolean result = false;
		
		if(toTest.toString().equals(m_type)){
			result = true;
		}
		return result;
	}

}
