package cn.com.analysys.javasdk;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author admin
 */
public class BatchCollecter implements Collecter {
    private final static int DEFAULT_BATCH_NUM = 20;
    private final static long DEFAULT_BATCH_SEC = 10;
    private final String serverUrl;
    private final int batchNum;
    private final long batchSec;
    private final boolean interrupt;
    private final List<Map<String, Object>> batchMsgList;
    private final ExecutorService singleThread;
    private long sendTimer = -1;
    private boolean isListen = true;
    private boolean debug;

    /**
     * 构造方法
     *
     * @param serverUrl 数据接收服务地址
     */
    public BatchCollecter(String serverUrl) {
        this(serverUrl, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC);
    }

    /**
     * 构造方法
     *
     * @param serverUrl 数据接收服务地址
     * @param interrupt 是否中断程序
     */
    public BatchCollecter(String serverUrl, boolean interrupt) {
        this(serverUrl, DEFAULT_BATCH_NUM, DEFAULT_BATCH_SEC, interrupt);
    }

    /**
     * 构造方法
     *
     * @param serverUrl 数据接收服务地址
     * @param batchNum  批量发送数量
     */
    public BatchCollecter(String serverUrl, int batchNum) {
        this(serverUrl, batchNum, DEFAULT_BATCH_SEC);
    }

    /**
     * 构造方法
     *
     * @param serverUrl 数据接收服务地址
     * @param batchNum  批量发送数量
     * @param batchSec  批量发送等待时间(秒)
     */
    public BatchCollecter(String serverUrl, int batchNum, long batchSec) {
        this(serverUrl, batchNum, batchSec, false);
    }

    /**
     * 构造方法
     *
     * @param serverUrl 数据接收服务地址
     * @param batchNum  批量发送数量
     * @param batchSec  批量发送等待时间(秒)
     * @param interrupt 是否中断程序
     */
    public BatchCollecter(String serverUrl, int batchNum, long batchSec, boolean interrupt) {
        if (serverUrl == null || serverUrl.trim().length() == 0) {
            throw new RuntimeException("Server URL is empty");
        } else {
            if (serverUrl.contains("/up")) {
                serverUrl = serverUrl.substring(0, serverUrl.indexOf("/up"));
            }
        }
        this.serverUrl = serverUrl + "/up";
        this.interrupt = interrupt;
        this.batchNum = batchNum;
        this.batchSec = batchSec * 1000;
        this.batchMsgList = new ArrayList<Map<String, Object>>(this.batchNum);
        this.singleThread = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        init();
    }

    private void init() {
        this.singleThread.execute(new Runnable() {
            @Override
            public void run() {
                while (isListen) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                    }
                    if (sendTimer != -1 && (System.currentTimeMillis() - sendTimer >= batchSec)) {
                        try {
                            upload();
                        } catch (Exception e) {
                        }
                    }
                }
            }
        });
    }

    @Override
    public boolean send(Map<String, Object> egCollectMessage) {
        synchronized (batchMsgList) {
            if (sendTimer == -1) {
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
            if (batchMsgList != null && batchMsgList.size() > 0) {
                try {
                    jsonData = ValidHandle.getEgJsonMapper().writeValueAsString(batchMsgList);
                    Map<String, String> headParam = new HashMap<String, String>(1);
                    if (debug) {
                        System.out.println(String.format("Send message to server: %s \ndata: %s", serverUrl, jsonData));
                    }
                    String retMsg = new MessageSender(serverUrl, headParam, jsonData).send();
                    if (debug) {
                        System.out.println(String.format("Send message success,response: %s", retMsg));
                    }
                } catch (JsonProcessingException e) {
                    if (interrupt) {
                        shutdown();
                        throw new RuntimeException("Json Serialize Error", e);
                    } else {
                        System.out.println("Json Serialize Error" + e);
                    }
                } catch (AnalysysException e) {
                    if (interrupt) {
                        shutdown();
                        throw new RuntimeException("Upload Data Error", e);
                    } else {
                        System.out.println("Upload Data Error" + e);
                    }
                } catch (IOException e) {
                    if (interrupt) {
                        shutdown();
                        throw new RuntimeException("Connect Server Error", e);
                    } else {
                        System.out.println("Connect Server Error" + e);
                    }
                } catch (Exception e) {
                    System.out.println("Send Data Error" + e);
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

    private void shutdown() {
        this.isListen = false;
        try {
            this.singleThread.shutdown();
            this.singleThread.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
    }

    private void resetTimer() {
        this.sendTimer = -1;
    }

    @Override
    public void debug(boolean debug) {
        this.debug = debug;
    }
}
