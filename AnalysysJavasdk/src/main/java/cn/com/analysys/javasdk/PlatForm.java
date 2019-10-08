package cn.com.analysys.javasdk;

/**
 * @author admin
 */
public enum PlatForm {
	//Java
	Java("Java"), 
	//python
	python("python"), 
	//JS
	JS("JS"), 
	//Node
	Node("Node"), 
	//PHP
	PHP( "PHP"), 
	//WeChat
	WeChat("WeChat"), 
	//Android
	Android("Android"), 
	//iOS
	iOS("iOS");
	private final String value; 
    private PlatForm(String value) { 
    	this.value = value; 
    }
    public String getValue() {
		return value;
	}
}
