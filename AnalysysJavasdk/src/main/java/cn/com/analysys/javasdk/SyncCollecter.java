package cn.com.analysys.javasdk;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author admin
 */
public class SyncCollecter implements Collecter {
    private final String serverUrl;
    private final boolean interrupt;
    private boolean debug;

    /**
     * 构造方法
     *
     * @param serverUrl 数据接收服务地址
     */
    public SyncCollecter(String serverUrl) {
        this(serverUrl, false);
    }

    /**
     * 构造方法
     *
     * @param serverUrl 数据接收服务地址
     * @param interrupt 是否中断程序
     */
    public SyncCollecter(String serverUrl, boolean interrupt) {
        if (serverUrl == null || serverUrl.trim().length() == 0) {
            throw new RuntimeException("Server URL is empty");
        } else {
            if (serverUrl.contains("/up")) {
                serverUrl = serverUrl.substring(0, serverUrl.indexOf("/up"));
            }
        }
        this.serverUrl = serverUrl + "/up";
        this.interrupt = interrupt;
    }

    @Override
    public boolean send(Map<String, Object> egCollectMessage) {
        String jsonData = null;
        try {
            List<Map<String, Object>> egMsgList = new ArrayList<Map<String, Object>>();
            egMsgList.add(egCollectMessage);
            jsonData = ValidHandle.getEgJsonMapper().writeValueAsString(egMsgList);
            Map<String, String> headParam = new HashMap<String, String>(1);
            if (debug) {
                System.out.println(String.format("Send message to server: %s \ndata: %s", serverUrl, jsonData));
            }
            String retMsg = new MessageSender(serverUrl, headParam, jsonData).send();
            if (debug) {
                System.out.println(String.format("Send message success,response: %s", retMsg));
            }
            return true;
        } catch (JsonProcessingException e) {
            if (interrupt) {
                throw new RuntimeException("Json Serialize Error: ", e);
            } else {
                System.out.println("Json Serialize Error: " + e);
            }
        } catch (AnalysysException e) {
            if (interrupt)
                throw new RuntimeException("Upload Data Error: ", e);
            else {
                System.out.println("Upload Data Error: " + e);
            }
        } catch (IOException e) {
            if (interrupt)
                throw new RuntimeException("Connect Server Error: ", e);
            else {
                System.out.println("Connect Server Error: " + e);
            }
        } catch (Exception e) {
            System.out.println("Sync Send Data Error: " + e);
        }
        return false;
    }

    @Override
    public void upload() {
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }

    @Override
    public void debug(boolean debug) {
        this.debug = debug;
    }
}
