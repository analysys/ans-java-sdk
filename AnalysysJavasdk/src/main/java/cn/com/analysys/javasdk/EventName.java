package cn.com.analysys.javasdk;

/**
 * @author admin
 */
public enum EventName {
	//profile_set
	P_SET("$profile_set"), 
	//profile_set_once
	P_SET_ONE("$profile_set_once"), 
	//profile_increment
	P_IN("$profile_increment"), 
	//profile_append
	P_APP("$profile_append"), 
	//profile_unset
	P_UN( "$profile_unset"), 
	//profile_delete
	P_DEL("$profile_delete"), 
	//alias
	ALIAS("$alias");
	private final String value; 
    private EventName(String value) { 
    	this.value = value; 
    }
    public String getValue() {
		return value;
	}
}
