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
	private final Boolean isEncode;
	private final String serverUrl;
	private final Map<String, String> egHeaderParams;
	private final String jsonData;
    
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param egHeaderParams HTTP消息头信息
	 * @param jsonData HTTP消息体
	 */
	public MessageSender(String serverUrl, Map<String, String> egHeaderParams, String jsonData){
		this(serverUrl, egHeaderParams, jsonData, true);
	}
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param egHeaderParams HTTP消息头信息
	 * @param jsonData HTTP消息体
	 * @param isEncode 是否对消息体进行编码,默认true
	 */
	public MessageSender(String serverUrl, Map<String, String> egHeaderParams, String jsonData, Boolean isEncode){
		this.serverUrl = serverUrl;
		this.egHeaderParams = egHeaderParams;
		this.jsonData = jsonData;
		this.isEncode = isEncode;
	}
	
	/**
	 * 发送消息至接收服务器
	 * @return HttpResponse
	 * @throws Exception Exception
	 */
	public String send() throws Exception {
		CloseableHttpClient httpclient = null;
		CloseableHttpResponse response = null;
		try {
			httpclient = getHttpClient();
			HttpPost egHttpPost = new HttpPost(this.serverUrl);
			egHttpPost.addHeader("User-Agent", "Analysys Java SDK");
			if (this.egHeaderParams != null) {
				for (Map.Entry<String, String> entry : this.egHeaderParams.entrySet()) {
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
			printLog(message, jsonData);
			if (httpStatusCode >= minCode && httpStatusCode < maxCode) {
				if(message != null && message.contains("\"code\":200")){
					return message;
				} else {
					throw new AnalysysException(message);
				}
			} else {
				throw new AnalysysException(message);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if(response != null)
				response.close();
			if(httpclient != null)
				httpclient.close();
		}
	}
	
	private void printLog(String message, String jsonData) {
		if(message != null && !message.contains("\"code\":200")){
			System.out.println("Data Upload Fail: " + jsonData);
		}
    }
	
    private CloseableHttpClient getHttpClient() {
    	return HttpClients.createDefault();
    }
    
    private RequestConfig getHttpConfig() {
    	RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(20000).setConnectTimeout(20000).build();
    	return requestConfig;
    }
}
