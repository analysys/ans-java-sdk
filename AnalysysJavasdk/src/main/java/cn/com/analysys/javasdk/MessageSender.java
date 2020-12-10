package cn.com.analysys.javasdk;

import java.util.Map;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * @author admin
 */
public class MessageSender {
	private final CloseableHttpClient httpclient;
	private final Boolean isEncode;
	private final String serverUrl;
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 */
	public MessageSender(String serverUrl){
		this(serverUrl, true);
	}
    
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param isEncode 是否对消息体进行编码,默认true
	 */
	public MessageSender(String serverUrl, Boolean isEncode){
		this.serverUrl = serverUrl;
		this.isEncode = isEncode;
		this.httpclient = getHttpClient();
	}
	
	/**
	 * 发送消息至接收服务器
	 * @param egHeaderParams HTTP消息头信息
	 * @param jsonData HTTP消息体
	 * @return HttpResponse
	 * @throws Exception Exception
	 */
	public String send(Map<String, String> egHeaderParams, String jsonData) throws Exception {
		CloseableHttpResponse response = null;
		try {
			HttpPost egHttpPost = new HttpPost(this.serverUrl);
			egHttpPost.addHeader("User-Agent", "Analysys Java SDK");
			if (egHeaderParams != null) {
				for (Map.Entry<String, String> entry : egHeaderParams.entrySet()) {
					egHttpPost.addHeader(entry.getKey(), entry.getValue());
				}
			}
			StringEntity egRequest = null;
			if(isEncode){
				egRequest = new StringEntity(AnalysysEncoder.encode(AnalysysEncoder.compress(jsonData)));
			} else {
				egRequest = new StringEntity(jsonData);
				egRequest.setContentType("application/json");
			}
			egRequest.setContentEncoding("UTF-8");
	        egHttpPost.setEntity(egRequest);
	        egHttpPost.setConfig(getHttpConfig());
	        response = httpclient.execute(egHttpPost);
			int httpStatusCode = response.getStatusLine().getStatusCode();
			int minCode = 200;
			int maxCode = 300;
			String message = EntityUtils.toString(response.getEntity(), "utf-8");
			try {
				message = AnalysysEncoder.uncompress(AnalysysEncoder.decode(message));
			} catch (Exception e) {}
			if(message != null && !success(message)){
				AnalysysLogger.print("Data Upload Fails: " + jsonData);
				AnalysysLogger.print("Data Upload FailsReason: " + message);
			}
			if (httpStatusCode >= minCode && httpStatusCode < maxCode) {
				if(message != null && success(message)){
					return message;
				}
			}
			return null;
		} catch (Throwable e) {
			AnalysysLogger.print("Data Upload Fail: " + jsonData);
			AnalysysLogger.print("Data Upload FailReason: " + e.getMessage());
			return null;
		} finally {
			if(response != null)
				try {
					response.close();
					response = null;
				} catch (Exception e2) {}
		}
	}
	
	private boolean success(String message){
		return message.toLowerCase().replaceAll(" ", "").contains("\"code\":200");
	}
	
	private static class SingletonClassInstance {
		private static CloseableHttpClient httpclient = HttpClients.createDefault();
		
		public static CloseableHttpClient getCloseableHttpClient(){
			return httpclient;
		}
	}
	
    private CloseableHttpClient getHttpClient() {
    	return SingletonClassInstance.getCloseableHttpClient();
    }
    
    private RequestConfig getHttpConfig() {
    	RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(20000).setConnectTimeout(20000).build();
    	return requestConfig;
    }
}
