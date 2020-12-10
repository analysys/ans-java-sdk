package cn.com.analysys.javasdk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author admin
 */
public class BatchCollecter implements Collecter {
	private final MessageSender sender;
	private long sendTimer = -1;
	private boolean isListen = true;
	private final String serverUrl;
	private final static int DEFAULT_BATCH_NUM = 20;
	private final static long DEFAULT_BATCH_SEC = 10;
	private final int batchNum;
	private final long batchSec;
	private final boolean interrupt;
	private final List<Map<String, Object>> batchMsgList;
	private ExecutorService singleThread;
	private boolean debug;
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 */
	public BatchCollecter(String serverUrl){
		this(serverUrl, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC);
	}
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param interrupt 是否中断程序
	 */
	public BatchCollecter(String serverUrl, boolean interrupt){
		this(serverUrl, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC, interrupt);
	}
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param batchNum 批量发送数量
	 */
	public BatchCollecter(String serverUrl, int batchNum){
		this(serverUrl, batchNum, DEFAULT_BATCH_SEC);
	}
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param batchNum 批量发送数量
	 * @param batchSec 批量发送等待时间(秒)
	 */
	public BatchCollecter(String serverUrl, int batchNum, long batchSec){
		this(serverUrl, batchNum, batchSec, false);
	}
	
	/**
	 * 构造方法
	 * @param serverUrl 数据接收服务地址
	 * @param batchNum 批量发送数量
	 * @param batchSec 批量发送等待时间(秒)
	 * @param interrupt 是否中断程序
	 */
	public BatchCollecter(String serverUrl, int batchNum, long batchSec, boolean interrupt){
		if(serverUrl == null || serverUrl.trim().length() == 0){
			throw new RuntimeException("Server URL is empty");
		} else {
			if(serverUrl.contains("/up")){
				serverUrl = serverUrl.substring(0, serverUrl.indexOf("/up"));
			}
		}
		this.serverUrl = serverUrl + "/up";
		this.interrupt = interrupt;
		this.batchNum = batchNum;
		this.batchSec = batchSec * 1000;
		this.batchMsgList = new ArrayList<Map<String, Object>>(this.batchNum);
		this.singleThread = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		this.sender = new MessageSender(this.serverUrl);
		init();
	}
	
	private void init(){
		this.singleThread.execute(new Runnable() {
			@Override
			public void run() {
				while(isListen){
					try { Thread.sleep(1000); } catch (Exception e1) {AnalysysLogger.print(e1.getMessage());}
					if (sendTimer != -1 && (System.currentTimeMillis() - sendTimer >= batchSec)) {
						try {
							upload();
						} catch (Exception e) {}
					}
				}
			}
		});
	}

	@Override
	public boolean send(Map<String, Object> egCollectMessage) {
		synchronized (batchMsgList) {
			if(sendTimer == -1){
				sendTimer = System.currentTimeMillis();
			}
			batchMsgList.add(egCollectMessage);
			String xWhat = "xwhat";
			if (batchMsgList.size() >= batchNum || EventName.ALIAS.getValue().equals(egCollectMessage.get(xWhat))) {
				upload();
			}
	    }
		return true;
	}

	@Override
	public void upload() {
		String jsonData = null;
		synchronized (batchMsgList) {
			if(batchMsgList != null && batchMsgList.size() > 0){
				try {
					jsonData = ValidHandle.getEgJsonMapper().writeValueAsString(batchMsgList);
					Map<String, String> headParam = new HashMap<String, String>(1);
					if(debug){
						AnalysysLogger.print(String.format("Send message to server: %s data: %s", serverUrl, jsonData));
					}
					String retMsg = this.sender.send(headParam, jsonData);
					if(debug && retMsg != null){
						AnalysysLogger.print(String.format("Send message success,response: %s", retMsg));
					}
				} catch (JsonProcessingException e) {
					AnalysysLogger.print("Json Serialization Fail: " + batchMsgList);
					if(interrupt){
						shutdown();
						throw new RuntimeException("Json Serialize Error: ", e);
					} else {
						AnalysysLogger.print("Json Serialize Error: " + e);
					}
				} catch (Exception e) {
					AnalysysLogger.print("Batch Send Data Error: " + e);
				} finally {
					batchMsgList.clear();
					resetTimer();
				}
			}
		}
	}
	
	@Override
	public void flush() {
		upload();
	}
	
	@Override
	public void close() {
		flush();
		shutdown();
	}
	
	private void shutdown(){
		this.isListen = false;
		try {
			this.singleThread.shutdown();
			this.singleThread.awaitTermination(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			AnalysysLogger.print(e.getMessage());
			this.singleThread = null;
		}
	}
	
	private void resetTimer(){
		this.sendTimer = -1;
	}

	@Override
	public void debug(boolean debug) {
		this.debug = debug;
	}
}
