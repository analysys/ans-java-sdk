package cn.com.analysys.javasdk;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author admin
 * Analysys Java SDK
 */
public class AnalysysJavaSdk {
	private final String SDK_VERSION = "4.1.2";
	private final Collecter collecter;
	private final String appId;
	private final Map<String, Object> egBaseProperties;
	private final Map<String, Object> xcontextSuperProperties;
	private int debugMode = DEBUG.CLOSE.getCode();
	
	/**
	 * 构造方法
	 * @param collecter 消息收集器
	 * @param appId 用户AppId
	 */
	public AnalysysJavaSdk(Collecter collecter, String appId){
		this(collecter, appId, false);
	}
	
	/**
	 * 构造方法
	 * @param collecter 消息收集器
	 * @param appId 用户AppId
	 * @param autoDelParam 是否自动删除校验不通过的属性
	 */
	public AnalysysJavaSdk(Collecter collecter, String appId, Boolean autoDelParam){
		this.collecter = collecter;
		if(appId == null || appId.trim().length() == 0){
			throw new RuntimeException("appKey is empty");
		}
		this.appId = appId;
		this.egBaseProperties = new HashMap<String, Object>(3);
		this.xcontextSuperProperties = new ConcurrentHashMap<String, Object>();
		ValidHandle.setDelNotValidParam(autoDelParam);
		initBaseProperties();
	}
	
	/**
	 * Debug模式
	 * @param debug Debug级别
	 */
	public boolean setDebugMode(DEBUG debug) {
		this.debugMode = debug.getCode();
		return true;
	}
	
	private boolean isDebug(){
		return debugMode == DEBUG.OPENNOSAVE.getCode() || debugMode == DEBUG.OPENANDSAVE.getCode();
	}
	
	/**
	 * 初始化基础属性
	 */
	public void initBaseProperties() {
	    this.egBaseProperties.clear();
	    this.egBaseProperties.put("$lib", PlatForm.Java.getValue());
	    this.egBaseProperties.put("$lib_version", SDK_VERSION);
	}
	
	/**
	 * 注册超级属性,注册后每次发送的消息体中都包含该属性值
	 * @param params 属性
	 */
	public boolean registerSuperProperties(Map<String, Object> params) throws AnalysysException {
		int num = 100;
		if(params.entrySet().size() > num){
			throw new AnalysysException("Too many super properties. max number is 100.");
		}
		ValidHandle.checkParam("", params);
		for(String key : params.keySet()){
			this.xcontextSuperProperties.put(key, params.get(key));
		}
		if(isDebug()){
			AnalysysLogger.print("registerSuperProperties success");
		}
		return true;
	}
	
	/**
	 * 移除超级属性
	 * @param key 属性key
	 */
	public boolean unRegisterSuperProperty(String key) {
		if(this.xcontextSuperProperties.containsKey(key)){
			this.xcontextSuperProperties.remove(key);
		}
		if(isDebug()){
			AnalysysLogger.print(String.format("unRegisterSuperProperty %s success", key));
		}
		return true;
	}
	
	/**
	 * 获取超级属性
	 * @param key 属性KEY
	 * @return 该KEY的超级属性值
	 */
	public Object getSuperPropertie(String key){
		if(this.xcontextSuperProperties.containsKey(key)){
			return this.xcontextSuperProperties.get(key);
		}
		return null;
	}
	
	/**
	 * 获取超级属性
	 * @return 所有超级属性
	 */
	public Map<String, Object> getSuperProperties(){
		return this.xcontextSuperProperties;
	}
	
	/**
	 * 清除超级属性
	 */
	public boolean clearSuperProperties(){
		this.xcontextSuperProperties.clear();
		if(isDebug()){
			AnalysysLogger.print("clearSuperProperties success");
		}
		return true;
	}
	
	/**
	 * 立即发送所有收集的信息到服务器
	 */
	public boolean flush() {
	    this.collecter.flush();
	    return true;
	}
	
	public void shutdown() {
	    this.collecter.close();
	}
	
	/**
	 * 设置用户的属性
	 * @param distinctId 用户ID
	 * @param isLogin 用户ID是否是登录 ID
	 * @param properties 用户属性
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean profileSet(String distinctId, boolean isLogin, Map<String, Object> properties, String platform) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_SET.getValue(), properties, platform, null);
	}
	public boolean profileSet(String distinctId, boolean isLogin, Map<String, Object> properties, String platform, String xwhen) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_SET.getValue(), properties, platform, xwhen);
	}
	
	/**
	 * 首次设置用户的属性,该属性只在首次设置时有效
	 * @param distinctId 用户ID
	 * @param isLogin 用户ID是否是登录 ID
	 * @param properties 用户属性
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean profileSetOnce(String distinctId, boolean isLogin, Map<String, Object> properties, String platform) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_SET_ONE.getValue(), properties, platform, null);
	}
	public boolean profileSetOnce(String distinctId, boolean isLogin, Map<String, Object> properties, String platform, String xwhen) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_SET_ONE.getValue(), properties, platform, xwhen);
	}
	
	/**
	 * 为用户的一个或多个数值类型的属性累加一个数值
	 * @param distinctId 用户ID
	 * @param isLogin 用户ID是否是登录 ID
	 * @param properties 用户属性
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean profileIncrement(String distinctId, boolean isLogin, Map<String, Object> properties, String platform) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_IN.getValue(), properties, platform, null);
	}
	public boolean profileIncrement(String distinctId, boolean isLogin, Map<String, Object> properties, String platform, String xwhen) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_IN.getValue(), properties, platform, xwhen);
	}
	
	/**
	 * 追加用户列表类型的属性
	 * @param distinctId 用户ID
	 * @param isLogin 用户ID是否是登录 ID
	 * @param properties 用户属性
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean profileAppend(String distinctId, boolean isLogin, Map<String, Object> properties, String platform) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_APP.getValue(), properties, platform, null);
	}
	public boolean profileAppend(String distinctId, boolean isLogin, Map<String, Object> properties, String platform, String xwhen) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_APP.getValue(), properties, platform, xwhen);
	}
	
	/**
	 * 删除用户某一个属性
	 * @param distinctId 用户ID
	 * @param isLogin 用户ID是否是登录 ID
	 * @param property 用户属性名称
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean profileUnSet(String distinctId, boolean isLogin, String property, String platform) throws AnalysysException {
		Map<String, Object> properties = new HashMap<String, Object>(2);
	    properties.put(property, "");
	    return upload(distinctId, isLogin, EventName.P_UN.getValue(), properties, platform, null);
	}
	public boolean profileUnSet(String distinctId, boolean isLogin, String property, String platform, String xwhen) throws AnalysysException {
		Map<String, Object> properties = new HashMap<String, Object>(2);
	    properties.put(property, "");
	    return upload(distinctId, isLogin, EventName.P_UN.getValue(), properties, platform, xwhen);
	}
	
	/**
	 * 删除用户所有属性
	 * @param distinctId 用户ID
	 * @param isLogin 用户ID是否是登录 ID
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean profileDelete(String distinctId,  boolean isLogin, String platform) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_DEL.getValue(), new HashMap<String, Object>(1), platform, null);
	}
	public boolean profileDelete(String distinctId,  boolean isLogin, String platform, String xwhen) throws AnalysysException {
		return upload(distinctId, isLogin, EventName.P_DEL.getValue(), new HashMap<String, Object>(1), platform, xwhen);
	}
	
	/**
	 * 关联用户匿名ID和登录ID
	 * @param aliasId 用户登录ID
	 * @param distinctId 用户匿名ID
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean alias(String aliasId, String distinctId, String platform) throws AnalysysException{
		Map<String, Object> param = new HashMap<String, Object>(2);
		param.put("$original_id", distinctId);
		return upload(aliasId, true, EventName.ALIAS.getValue(), param, platform, null);
	}
	public boolean alias(String aliasId, String distinctId, String platform, String xwhen) throws AnalysysException{
		Map<String, Object> param = new HashMap<String, Object>(2);
		param.put("$original_id", distinctId);
		return upload(aliasId, true, EventName.ALIAS.getValue(), param, platform, xwhen);
	}
	
	/**
	 * 追踪用户多个属性的事件
	 * @param distinctId 用户ID
	 * @param isLogin 用户ID是否是登录 ID
	 * @param eventName 事件名称
	 * @param properties 事件属性
	 * @param platform 平台类型
	 * @throws AnalysysException 自定义异常
	 */
	public boolean track(String distinctId, boolean isLogin, String eventName, Map<String, Object> properties, String platform) throws AnalysysException {
		return upload(distinctId, isLogin, eventName, properties, platform, null);
	}
	public boolean track(String distinctId, boolean isLogin, String eventName, Map<String, Object> properties, String platform, String xwhen) throws AnalysysException {
		return upload(distinctId, isLogin, eventName, properties, platform, xwhen);
	}
	
	/**
	 * 上传数据,首先校验相关KEY和VALUE,符合规则才可以上传
	 * @param distinctId 用户标识
	 * @param isLogin 是否登陆
	 * @param eventName 事件名称
	 * @param properties 属性
	 * @param platform 平台类型
	 * @param xwhen 时间戳
	 * @throws AnalysysException 自定义异常
	 */
	private boolean upload(String distinctId, boolean isLogin, String eventName, Map<String, Object> properties, String platform, String xwhen) throws AnalysysException {
		HashMap<String, Object> targetProperties = new HashMap<String, Object>();
		if(properties != null){
			targetProperties.putAll(properties);
		}
		ValidHandle.checkProperty(distinctId, eventName, targetProperties, this.xcontextSuperProperties.size());
		Map<String, Object> eventMap = new HashMap<String, Object>(8);
		eventMap.put("xwho", distinctId);
		if(xwhen != null && xwhen.trim().length() > 0){
			if(xwhen.trim().length() != 13){
				throw new AnalysysException(String.format("The param xwhen %s not a millisecond timestamp.", xwhen.trim()));
			}
			try {
				long when = Long.valueOf(xwhen.trim());
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date(when));
				eventMap.put("xwhen", when);
			} catch (Exception e) {
				throw new AnalysysException(String.format("The param xwhen %s not a timestamp.", xwhen.trim()));
			}
		} else {
			if(EventName.ALIAS.getValue().equals(eventName)){
				eventMap.put("xwhen", System.currentTimeMillis() - 3);
			} else {
				eventMap.put("xwhen", System.currentTimeMillis());
			}
		}
		eventMap.put("xwhat", eventName);
		eventMap.put("appid", appId);
		Map<String, Object> newProperties = new HashMap<String, Object>(16);
		String profile = "$profile";
		if(!(eventName != null && (eventName.startsWith(profile) || eventName.startsWith(EventName.ALIAS.getValue())))){
			newProperties.putAll(this.xcontextSuperProperties);
		}
		newProperties.put("$debug", debugMode);
		if(targetProperties != null){
			newProperties.putAll(targetProperties);
		}
		newProperties.putAll(egBaseProperties);
		newProperties.put("$is_login", isLogin);
		String newPlatForm = getPlatForm(platform);
		if(newPlatForm != null && newPlatForm.trim().length() > 0){
			newProperties.put("$platform", newPlatForm);
		}
		eventMap.put("xcontext", newProperties);
		this.collecter.debug(isDebug());
		boolean ret = this.collecter.send(eventMap);
		if(eventName != null && eventName.startsWith(profile) && isDebug() && ret){
			AnalysysLogger.print(String.format("%s success.", eventName));
		}
		return ret;
	}
	
	private String getPlatForm(String platform){
    	if(PlatForm.JS.getValue().equalsIgnoreCase(platform)) {return PlatForm.JS.getValue();}
    	if(PlatForm.WeChat.getValue().equalsIgnoreCase(platform)) {return PlatForm.WeChat.getValue();}
    	if(PlatForm.Android.getValue().equalsIgnoreCase(platform)) {return PlatForm.Android.getValue();}
    	if(PlatForm.iOS.getValue().equalsIgnoreCase(platform)) {return PlatForm.iOS.getValue();}
    	AnalysysLogger.print(String.format("Warning: param platform:%s  Your input are not:iOS/Android/JS/WeChat.", platform == null ? "" : platform));
    	if(PlatForm.Java.getValue().equalsIgnoreCase(platform)) {return PlatForm.Java.getValue();}
    	if(PlatForm.python.getValue().equalsIgnoreCase(platform)) {return PlatForm.python.getValue();}
    	if(PlatForm.Node.getValue().equalsIgnoreCase(platform)) {return PlatForm.Node.getValue();}
    	if(PlatForm.PHP.getValue().equalsIgnoreCase(platform)) {return PlatForm.PHP.getValue();}
    	if(platform == null || platform.trim().length() == 0) {
    		AnalysysLogger.print(String.format("Warning: param platform is empty, will use default value: %s.", PlatForm.Java.getValue()));
    		return PlatForm.Java.getValue();
    	}
    	return platform;
    }
}
