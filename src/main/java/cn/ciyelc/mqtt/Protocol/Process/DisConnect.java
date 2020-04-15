package cn.ciyelc.mqtt.Protocol.Process;
import cn.ciyelc.mqtt.model.SessionStore;
import cn.ciyelc.mqtt.server.DupPubRelMessageStoreService;
import cn.ciyelc.mqtt.server.DupPublishMessageStoreService;
import cn.ciyelc.mqtt.server.SessionStoreService;
import cn.ciyelc.mqtt.server.SubscribeStoreService;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

/**
 * 断开连接
 */
@Slf4j
public class DisConnect {
    private SessionStoreService sessionStoreService;
    private SubscribeStoreService subscribeStoreService;
    private DupPublishMessageStoreService dupPublishMessageStoreService;
    private DupPubRelMessageStoreService dupPubRelMessageStoreService;

    public DisConnect(SessionStoreService sessionStoreService, SubscribeStoreService subscribeStoreService, DupPublishMessageStoreService dupPublishMessageStoreService, DupPubRelMessageStoreService dupPubRelMessageStoreService) {
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
    }

    public void processDisConnect(Channel channel){
        String clientId = (String) channel.attr(AttributeKey.valueOf("clientId")).get();
        SessionStore sessionStore = sessionStoreService.get(clientId);
        if (sessionStore!=null && sessionStore.isCleanSession()){
            subscribeStoreService.removeForClient(clientId);
            dupPublishMessageStoreService.removeByClient(clientId);
            dupPubRelMessageStoreService.removeByClient(clientId);
        }
        log.info("DISCONNECT - clientId: {}, cleanSession: {}", clientId, sessionStore.isCleanSession());
        sessionStoreService.remove(clientId);
        channel.close();
    }
}