package cn.com.analysys.javasdk;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author admin
 */
public class ValidHandle {
	private static boolean delNotValidParam = true;
	private static ObjectMapper egJsonMapper = ValidHandle.getJsonObjectMapper();
	private static final Pattern KEY_PATTERN = Pattern.compile("^((?!^xwhat$|^xwhen$|^xwho$|^appid$|^xcontext$|^\\$lib$|^\\$lib_version$)^[$a-zA-Z][$a-zA-Z0-9_]{0,98})$", Pattern.CASE_INSENSITIVE);
	private static final Pattern KEY_PATTERN_CONTEXT = Pattern.compile("^((?!^xwhat$|^xwhen$|^xwho$|^appid$|^xcontext$|^\\$lib$|^\\$lib_version$)^[$a-zA-Z][$a-zA-Z0-9_]{0,124})$", Pattern.CASE_INSENSITIVE);

	public static void setDelNotValidParam(boolean delNotValidParam) {
		ValidHandle.delNotValidParam = delNotValidParam;
	}

	public static ObjectMapper getEgJsonMapper() {
		return egJsonMapper;
	}

	/**
	 * 属性参数格式校验
	 * @param eventName 事件名称
	 * @param properties 属性
	 * @throws AnalysysException 自定义异常
	 */
	public static void checkParam(String eventName, Map<String, Object> properties) throws AnalysysException {
		if(properties == null){
			properties = new HashMap<String, Object>(1);
		}
		for(Iterator<Map.Entry<String, Object>> it = properties.entrySet().iterator(); it.hasNext();){
			Map.Entry<String, Object> property = it.next();
			if(property.getKey() == null || property.getKey().toString().trim().length() == 0){
				System.out.println(String.format("Warn: The property key is null or empty."));
				if(property.getKey() == null){
					it.remove();
					continue;
				}
			}
			if(property.getValue() == null)
				continue;
			try {
				if(property != null)
					checkParamImpl(eventName, property);
			} catch (AnalysysException e) {
				if(delNotValidParam) {
					it.remove();
					System.out.println(e);
				} else {
					throw e;
				}
			}
		}
	}
	
	private static boolean checkParamImpl(String eventName, Map.Entry<String, Object> property) throws AnalysysException {
		int valueLength = 8192;
		int valueWarnLength = 255;
		int keyLength = 99;
		int valueListLen = 100;
		String piEventName = "$profile_increment";
		String paEventName = "$profile_append";
		//key约束 符合java命名规则： 开头约束:字母或者$ 字符类型:大小写字母、数字、下划线和 $ 最大长度125字符
		if (property.getKey().length() > keyLength) {
			System.out.println(String.format("Warn: The property key %s is too long, max length is %s.", property.getKey(), keyLength));
		}
		if (!(KEY_PATTERN_CONTEXT.matcher(property.getKey()).matches())) {
			System.out.println(String.format("Warn: The property key %s is invalid.", property.getKey()));
		}
		if (!(property.getValue() instanceof Number) && 
				!(property.getValue() instanceof Boolean) && 
				!(property.getValue() instanceof String) && 
				!property.getValue().getClass().isArray() && 
		        !(property.getValue() instanceof List<?>)) {
			System.out.println(String.format("Warn: The property %s is not Number, String, Boolean, List<String>.", property.getKey()));
		}
		if (property.getValue() instanceof String && property.getValue().toString().length() == 0) {
			System.out.println(String.format("Warn: The property %s String value is null or empty.", property.getKey()));
		}
		if (property.getValue() instanceof String && property.getValue().toString().length() > valueWarnLength) {
			System.out.println(String.format("Warn: The property %s String value is too long, max length is %s.", property.getKey(), valueWarnLength));
		}
		if (property.getValue() instanceof String && property.getValue().toString().length() > valueLength) {
			property.setValue(property.getValue().toString().substring(0, valueLength-1) + "$");
		}
		//数组集合约束 数组或集合内最多包含100条,若为字符串数组或集合,每条最大长度255个字符
		if (property.getValue() instanceof List<?>) {
			List<?> valueList = (List<?>)property.getValue();
			if(valueList.size() > valueListLen){
				valueList = valueList.subList(0, valueListLen);
				property.setValue(valueList);
				System.out.println(String.format("Warn: The property %s, max number should be %s.", property.getKey(), valueListLen));
			}
			List<Object> list = (List<Object>)property.getValue();
			if(list != null){
				if(list.size() == 0){
					System.out.println(String.format("Warn: The property %s is empty.", property.getKey()));
				}
				for (ListIterator<Object> iterator = list.listIterator(); iterator.hasNext();) {
					Object vals = iterator.next();
					if(vals == null)
						continue;
					if (!(vals instanceof String)) {
						System.out.println(String.format("The property %s should be a list of String.", property.getKey()));
					}
					if (((String) vals).length() > valueWarnLength) {
						System.out.println(String.format("Warn: The property %s some value is too long, max length is %s.", property.getKey(), valueWarnLength));
					}
					if (((String) vals).length() > valueLength) {
						iterator.set(vals.toString().substring(0, valueLength-1) + "$");
					}
				}
			}
		}
		if (piEventName.equals(eventName) && !(property.getValue() instanceof Number)) {
			System.out.println(String.format("The property value of %s should be a Number.", property.getKey()));
		}
		if (paEventName.equals(eventName)) {
			if (!(property.getValue() instanceof List<?>) && !property.getValue().getClass().isArray()) {
				System.out.println(String.format("The property value of %s should be a List<String>.", property.getKey()));
			}
		}
		return true;
	}
	
	/**
	 * 格式校验
	 * @param distinctId 用户标识
	 * @param eventName 事件名称
	 * @param properties 属性
	 * @param commProLen 公共属性长度
	 * @throws AnalysysException 自定义异常
	 */
	public static void checkProperty(String distinctId, String eventName, Map<String, Object> properties, int commProLen) throws AnalysysException {
		String aliasEventName = "$alias";
		String originalId = "$original_id";
		int eventNameLen = 99;
		int idLength = 255;
		if(properties == null){
			properties = new HashMap<String, Object>(1);
		}
		if (distinctId == null || distinctId.length() == 0) {
			throw new AnalysysException(String.format("aliasId %s is empty.", distinctId));
		}
		if (distinctId.toString().length() > idLength) {
			throw new AnalysysException(String.format("aliasId %s is too long, max length is %s.", distinctId, idLength));
		}
		if (aliasEventName.equals(eventName)) {
			if (properties.get(originalId) == null || properties.get(originalId).toString().length() == 0) {
				throw new AnalysysException(String.format("original_id %s is empty.", properties.get(originalId)));
			}
			if (properties.get(originalId).toString().length() > idLength) {
		    	throw new AnalysysException(String.format("original_id %s is too long, max length is %s.", properties.get(originalId), idLength));
		    }
		}
		if (eventName != null) {
			if (eventName.length() == 0) {
				System.out.println("EventName is empty.");
		}
		if (eventName.length() > eventNameLen) {
			System.out.println(String.format("EventName %s is too long, max length is %s.", eventName, eventNameLen));
		}
		if (!(KEY_PATTERN.matcher(eventName).matches())) {
			System.out.println(String.format("EventName %s is invalid.", eventName));
		}
		} else {
			System.out.println("EventName is null.");
		}
		checkParam(eventName, properties);
	}
	
	private static ObjectMapper getJsonObjectMapper() {
	    ObjectMapper jsonObjectMapper = new ObjectMapper();
	    jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	    jsonObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL); //允许空: ALWAYS
	    return jsonObjectMapper;
	}
}
