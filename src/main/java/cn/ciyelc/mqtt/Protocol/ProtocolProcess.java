package cn.ciyelc.mqtt.Protocol;

import cn.ciyelc.mqtt.Protocol.Process.*;
import cn.ciyelc.mqtt.server.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * MQTT协议处理
 */
@Component
public class ProtocolProcess {
    @Autowired
    private SessionStoreService sessionStoreService;
    @Autowired
    private SubscribeStoreService subscribeStoreService;
    @Autowired
    private DupPublishMessageStoreService dupPublishMessageStoreService;
    @Autowired
    private DupPubRelMessageStoreService dupPubRelMessageStoreService;
    @Autowired
    private RetainMessageStoreService retainMessageStoreService;
    private Connect connect;

    private DisConnect disConnect;

    private PingReq pingReq;

    private Subscribe subscribe;

    private UnSubscribe unSubscribe;

    private Publish publish;

    private PubAck pubAck;

    private PubRel pubRel;

    private PubRec pubRec;

    private PubComp pubComp;

    public Connect connect(){
        if (connect == null){
            connect = new Connect(sessionStoreService,subscribeStoreService,dupPublishMessageStoreService,dupPubRelMessageStoreService);
        }
        return connect;
    }

    public DisConnect disConnect() {
        if (disConnect == null) {
            disConnect = new DisConnect(sessionStoreService, subscribeStoreService, dupPublishMessageStoreService, dupPubRelMessageStoreService);
        }
        return disConnect;
    }

    public PingReq pingReq() {
        if (pingReq == null) {
            pingReq = new PingReq();
        }
        return pingReq;
    }

    public Subscribe subscribe(){
        if (subscribe == null){
            subscribe = new Subscribe(subscribeStoreService,retainMessageStoreService);
        }
        return subscribe;
    }

    public UnSubscribe unSubscribe() {
        if (unSubscribe == null) {
            unSubscribe = new UnSubscribe(subscribeStoreService);
        }
        return unSubscribe;
    }

    public Publish publish() {
        if (publish == null) {
            publish = new Publish(sessionStoreService, subscribeStoreService, retainMessageStoreService, dupPublishMessageStoreService);
        }
        return publish;
    }



    public PubRel pubRel() {
        if (pubRel == null) {
            pubRel = new PubRel();
        }
        return pubRel;
    }

    public PubAck pubAck() {
        if (pubAck == null) {
            pubAck = new PubAck(dupPublishMessageStoreService);
        }
        return pubAck;
    }

    public PubRec pubRec() {
        if (pubRec == null) {
            pubRec = new PubRec(dupPublishMessageStoreService, dupPubRelMessageStoreService);
        }
        return pubRec;
    }

    public PubComp pubComp() {
        if (pubComp == null) {
            pubComp = new PubComp(dupPubRelMessageStoreService);
        }
        return pubComp;
    }

    public SessionStoreService getSessionStoreService() {
        return sessionStoreService;
    }
}