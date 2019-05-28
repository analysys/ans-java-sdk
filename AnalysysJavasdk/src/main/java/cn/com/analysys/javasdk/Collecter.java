package cn.com.analysys.javasdk;

import java.util.Map;

/**
 * @author admin
 */
public interface Collecter {
    /**
     * 发送/缓存消息
     *
     * @param message 消息
     * @return 是否成功
     */
    boolean send(Map<String, Object> message);

    /**
     * 上报数据
     */
    void upload();

    /**
     * 刷新缓存
     */
    void flush();

    /**
     * 关闭
     */
    void close();

    /**
     * DEBUG
     *
     * @param debug 是否debug
     */
    void debug(boolean debug);
}
