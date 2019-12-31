package cn.com.analysys.javasdk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * @author admin
 */
public class LogCollecter implements Collecter {
	private final String linefeed = "\n";
	private final String logFolder;
	private final SimpleDateFormat format;
	private boolean async = false;
	private boolean singleObj = true;
	private final static int RETRY_TIMES = 3; //重试3次
	private final static int DEFAULT_BATCH_NUM = 20;
	private final static long DEFAULT_BATCH_SEC = 10;
	private long sendTimer = -1;
	private List<Map<String, Object>> batchMsgList;
	private FileOutputStream lockstream = null;
	private ExecutorService singleThread;
	private boolean isListen = true;
	private int batchNum;
	private long batchSec;
	
	public LogCollecter(String logFolder){
		this(logFolder, GeneralRule.HOUR, false, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC, true);
	}
	
	public LogCollecter(String logFolder, boolean async){
		this(logFolder, GeneralRule.HOUR, async, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC, true);
	}
	
	public LogCollecter(String logFolder, GeneralRule rule){
		this(logFolder, rule, false, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC, true);
	}
	
	public LogCollecter(String logFolder, GeneralRule rule, boolean async){
		this(logFolder, rule, async, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC, true);
	}
	
	public LogCollecter(String logFolder, GeneralRule rule, boolean async, int batchNum, long batchSec){
		this(logFolder, rule, async, batchNum, batchSec, true);
	}
	
	private LogCollecter(String logFolder, GeneralRule rule, boolean async, int batchNum, long batchSec, boolean singleObj){
		this.logFolder = logFolder;
		if(!new File(logFolder).exists()){
			new File(logFolder).mkdirs();
		}
		String lock = "lock";
		if(GeneralRule.DAY.equals(rule)){
    		this.format = new SimpleDateFormat("yyyyMMdd");
    		lock = lock.concat("_day");
    	} else {
    		this.format = new SimpleDateFormat("yyyyMMddHH");
    		lock = lock.concat("_hour");
    	}
		this.async = async;
		this.singleObj = singleObj;
		this.batchMsgList = new ArrayList<Map<String, Object>>(1);
		if(this.async){
			this.batchNum = batchNum;
			this.batchSec = batchSec * 1000;
			this.batchMsgList = new ArrayList<Map<String, Object>>(this.batchNum);
			this.singleThread = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
			init();
		}
		if(System.getProperty("os.name").toLowerCase().startsWith("win")){
			String lockFileName = logFolder.concat(lock);
			if(!logFolder.endsWith(File.separator))
				lockFileName = logFolder.concat(File.separator).concat(lock);
			try {
				if(!new File(lockFileName).exists()){
					new File(lockFileName).createNewFile();
				}
				lockstream = new FileOutputStream(lockFileName, true);
			} catch (Exception e) {
				System.out.println("Init LockStream Error: " + e);
			}
		}
	}

	@Override
	public boolean send(Map<String, Object> egCollectMessage) {
		try {
			if(!async){
				List<Map<String, Object>> egMsgList = new ArrayList<Map<String, Object>>();
				egMsgList.add(egCollectMessage);
				dealLog(egMsgList);
			} else {
				synchronized (batchMsgList) {
					if(sendTimer == -1)
						sendTimer = System.currentTimeMillis();
					batchMsgList.add(egCollectMessage);
			    }
				if (batchMsgList.size() >= batchNum) {
					upload();
				}
			}
		} catch (Exception e) {
			try {
				System.out.println("Log Data Error: " + serialize(egCollectMessage));
			} catch (Exception e1) {}
		}
		return false;
	}

	@Override
	public void upload() {
		synchronized (batchMsgList) {
			if(batchMsgList != null && batchMsgList.size() > 0){
				try {
					dealLog(batchMsgList);
				} catch (JsonProcessingException e) {
					System.out.println("Json Serialize Error: " + e);
				} catch (IOException e) {
					System.out.println("Json Serialize Error: " + e);
				} finally {
					batchMsgList.clear();
					if(this.async)
						resetTimer();
				}
			}
		}
	}
	
	private void dealLog(List<Map<String, Object>> batchMsgList) throws JsonGenerationException, JsonMappingException, IOException{
		boolean success = false;
		if(singleObj){
			StringBuilder sb = new StringBuilder();
			int index = 0;
			for(Map<String, Object> map : batchMsgList){
				String jsonData = serialize(map);
				if(++index > 1)
					sb.append(linefeed);
				sb.append(jsonData);
			}
			success = dealLog(sb.toString());
		} else {
			String jsonData = serialize(batchMsgList);
			success = dealLog(jsonData);
		}
		if(!success)
			System.out.println("Error After Retry " + RETRY_TIMES + " Times: " + serialize(batchMsgList));
	}
	
	private String serialize(Object obj) throws JsonGenerationException, JsonMappingException, IOException{
		return ValidHandle.getEgJsonMapper().writeValueAsString(obj);
	}
	
	private boolean dealLog(String jsonData) {
		boolean success = write(jsonData);
		if(!success) {
			int total = RETRY_TIMES;
			while(!success && total-- > 0){
				try { Thread.sleep(1000); } catch (Exception e1) {System.out.println(e1);}
				success = write(jsonData);
			}
		}
		return success;
	}
	
	private boolean write(String jsonData) {
		try {
			boolean success = LogWriter.write(logFolder, generalNowTime(), jsonData.concat(linefeed), lockstream);
			return success;
		} catch (Exception e) {
			return false;
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

	@Override
	public void debug(boolean debug) {}
	
	private void init(){
		this.singleThread.execute(new Runnable() {
			@Override
			public void run() {
				while(isListen){
					try { Thread.sleep(1000); } catch (Exception e1) {System.out.println(e1);}
					if (sendTimer != -1 && (System.currentTimeMillis() - sendTimer >= batchSec)) {
						try {
							upload();
						} catch (Exception e) {}
					}
				}
			}
		});
	}
	
	private void shutdown(){
		this.isListen = false;
		try {
			LogWriter.close();
			if(this.async){
				this.singleThread.shutdown();
				this.singleThread.awaitTermination(5, TimeUnit.SECONDS);
			}
		} catch (Exception e) {
			System.out.println(e);
			this.singleThread = null;
		}
	}
	
	private void resetTimer(){
		this.sendTimer = -1;
	}
	
	public String generalNowTime() {
		return format.format(new Date());
    }
    
    public static enum GeneralRule {
    	HOUR, DAY;
    }
}
