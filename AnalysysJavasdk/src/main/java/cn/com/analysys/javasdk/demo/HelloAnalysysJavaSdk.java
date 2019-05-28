package cn.com.analysys.javasdk.demo;

import cn.com.analysys.javasdk.AnalysysException;
import cn.com.analysys.javasdk.AnalysysJavaSdk;
import cn.com.analysys.javasdk.DEBUG;
import cn.com.analysys.javasdk.SyncCollecter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HelloAnalysysJavaSdk {
    private final static String APP_ID = "1234";
    private final static String ANALYSYS_SERVICE_URL = "http://sdk.analysys.cn:8089";

    public static void main(String[] args) {
        AnalysysJavaSdk analysys = new AnalysysJavaSdk(new SyncCollecter(ANALYSYS_SERVICE_URL), APP_ID);
        //AnalysysJavaSdk analysys = new AnalysysJavaSdk(new BatchCollecter(ANALYSYS_SERVICE_URL), APP_ID);
        try {
            String distinctId = "1234567890987654321";
            String platForm = "android"; //Android平台
            analysys.setDebugMode(DEBUG.CLOSE); //设置debug模式
            //浏览商品
            Map<String, Object> trackPropertie = new HashMap<String, Object>();
            trackPropertie.put("$ip", "112.112.112.112"); //IP地址
            List<String> bookList = new ArrayList<String>();
            bookList.add("Thinking in Java");
            trackPropertie.put("productName", bookList);  //商品列表
            trackPropertie.put("productType", "Java书籍");//商品类别
            trackPropertie.put("producePrice", 80);          //商品价格
            trackPropertie.put("shop", "xx网上书城");     //店铺名称
            analysys.track(distinctId, false, "ViewProduct", trackPropertie, platForm);

            //用户注册登录
            String registerId = "ABCDEF123456789";
            analysys.alias(registerId, distinctId, platForm);

            //设置公共属性
            Map<String, Object> superPropertie = new HashMap<String, Object>();
            superPropertie.put("sex", "male"); //性别
            superPropertie.put("age", 23);     //年龄
            analysys.registerSuperProperties(superPropertie);
            //用户信息
            Map<String, Object> profiles = new HashMap<String, Object>();
            profiles.put("$city", "北京");        //城市
            profiles.put("$province", "北京");  //省份
            profiles.put("nickName", "昵称123");//昵称
            profiles.put("userLevel", 0);        //用户级别
            profiles.put("userPoint", 0);        //用户积分
            List<String> interestList = new ArrayList<String>();
            interestList.add("户外活动");
            interestList.add("足球赛事");
            interestList.add("游戏");
            profiles.put("interest", interestList);//用户兴趣爱好
            analysys.profileSet(registerId, true, profiles, platForm);

            //用户注册时间
            Map<String, Object> profile_age = new HashMap<String, Object>();
            profile_age.put("registerTime", "20180101101010");
            analysys.profileSetOnce(registerId, true, profile_age, platForm);

            //重新设置公共属性
            analysys.clearSuperProperties();
            superPropertie.clear();
            superPropertie = new HashMap<String, Object>();
            superPropertie.put("userLevel", 0); //用户级别
            superPropertie.put("userPoint", 0); //用户积分
            analysys.registerSuperProperties(superPropertie);

            //再次浏览商品
            trackPropertie.clear();
            trackPropertie.put("$ip", "112.112.112.112"); //IP地址
            List<String> abookList = new ArrayList<String>();
            abookList.add("Thinking in Java");
            trackPropertie.put("productName", bookList);  //商品列表
            trackPropertie.put("productType", "Java书籍");//商品类别
            trackPropertie.put("producePrice", 80);          //商品价格
            trackPropertie.put("shop", "xx网上书城");     //店铺名称
            analysys.track(registerId, true, "ViewProduct", trackPropertie, platForm);

            //订单信息
            trackPropertie.clear();
            trackPropertie.put("orderId", "ORDER_12345");
            trackPropertie.put("price", 80);
            analysys.track(registerId, true, "Order", trackPropertie, platForm);

            //支付信息
            trackPropertie.clear();
            trackPropertie.put("orderId", "ORDER_12345");
            trackPropertie.put("productName", "Thinking in Java");
            trackPropertie.put("productType", "Java书籍");
            trackPropertie.put("producePrice", 80);
            trackPropertie.put("shop", "xx网上书城");
            trackPropertie.put("productNumber", 1);
            trackPropertie.put("price", 80);
            trackPropertie.put("paymentMethod", "AliPay");
            analysys.track(registerId, true, "Payment", trackPropertie, platForm);
        } catch (AnalysysException e) {
            e.printStackTrace();
            analysys.flush();
        }
    }
}
