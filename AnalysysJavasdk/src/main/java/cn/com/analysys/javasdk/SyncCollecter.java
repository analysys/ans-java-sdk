package cn.com.analysys.javasdk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author admin
 */
public class SyncCollecter implements Collecter {
	private final MessageSender sender;
	private final String serverUrl;
	private final boolean interrupt;
	private boolean debug;
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 */
	public SyncCollecter(String serverUrl){
		this(serverUrl, false);
	}
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param interrupt 是否中断程序
	 */
	public SyncCollecter(String serverUrl, boolean interrupt){
		if(serverUrl == null || serverUrl.trim().length() == 0){
			throw new RuntimeException("Server URL is empty");
		} else {
			if(serverUrl.contains("/up")){
				serverUrl = serverUrl.substring(0, serverUrl.indexOf("/up"));
			}
		}
		this.serverUrl = serverUrl + "/up";
		this.interrupt = interrupt;
		this.sender = new MessageSender(this.serverUrl);
	}

	@Override
	public boolean send(Map<String, Object> egCollectMessage) {
		String jsonData = null;
		List<Map<String, Object>> egMsgList = new ArrayList<Map<String, Object>>();
		try {
			egMsgList.add(egCollectMessage);
			jsonData = ValidHandle.getEgJsonMapper().writeValueAsString(egMsgList);
			Map<String, String> headParam = new HashMap<String, String>(1);
			if(debug){
				AnalysysLogger.print(String.format("Send message to server: %s data: %s", serverUrl, jsonData));
			}
			String retMsg = this.sender.send(headParam, jsonData);
			if(debug && retMsg != null){
				AnalysysLogger.print(String.format("Send message success,response: %s", retMsg));
			}
			return retMsg != null;
		} catch (JsonProcessingException e) {
			AnalysysLogger.print("Json Serialization Fail: " + egMsgList);
			if(interrupt){
				throw new RuntimeException("Json Serialize Error: ", e);
			} else {
				AnalysysLogger.print("Json Serialize Error: " + e);
			}
		} catch (Exception e) {
			AnalysysLogger.print("Sync Send Data Error: " + e);
		}
		return false;
	}

	@Override
	public void upload() { }
	@Override
	public void flush() { }
	@Override
	public void close() { }

	@Override
	public void debug(boolean debug) {
		this.debug = debug;
	}
}
