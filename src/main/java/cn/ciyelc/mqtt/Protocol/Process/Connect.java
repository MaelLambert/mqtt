package cn.ciyelc.mqtt.Protocol.Process;
import cn.ciyelc.mqtt.model.DupPubRelMessageStore;
import cn.ciyelc.mqtt.model.DupPublishMessageStore;
import cn.ciyelc.mqtt.model.SessionStore;
import cn.ciyelc.mqtt.server.DupPubRelMessageStoreService;
import cn.ciyelc.mqtt.server.DupPublishMessageStoreService;
import cn.ciyelc.mqtt.server.SessionStoreService;
import cn.ciyelc.mqtt.server.SubscribeStoreService;
import cn.hutool.core.util.StrUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 处理连接
 */
@Slf4j
public class Connect {
    private SessionStoreService sessionStoreService;
    private SubscribeStoreService subscribeStoreService;
    private DupPublishMessageStoreService dupPublishMessageStoreService;
    private DupPubRelMessageStoreService dupPubRelMessageStoreService;

    public Connect(SessionStoreService sessionStoreService, SubscribeStoreService subscribeStoreService,DupPublishMessageStoreService dupPublishMessageStoreService, DupPubRelMessageStoreService dupPubRelMessageStoreService) {
        this.sessionStoreService = sessionStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.subscribeStoreService = subscribeStoreService;
    }

    public void processConnect(Channel channel, MqttConnectMessage mqttMessage) {
        // 消息解码器出现异常
        if (mqttMessage.decoderResult().isFailure()) {
            Throwable cause = mqttMessage.decoderResult().cause();
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                // 不支持的协议版本
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false), null);
                channel.writeAndFlush(connAckMessage);
                channel.close();
                return;
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // 不合格的clientId
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false), null);
                channel.writeAndFlush(connAckMessage);
                channel.close();
                return;
            }
            channel.close();
            return;
        }

        //客户端必须提供clientId.
        if (StrUtil.isBlank(mqttMessage.payload().clientIdentifier())) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false), null);
            channel.writeAndFlush(connAckMessage);
            channel.close();
            return;
        }

        //处理连接心跳包
        if (mqttMessage.variableHeader().keepAliveTimeSeconds() > 0){
            if (channel.pipeline().names().contains("idle")){
                channel.pipeline().remove("idle");
            }
            channel.pipeline().addFirst("idle",new IdleStateHandler(0, 0, Math.round(mqttMessage.variableHeader().keepAliveTimeSeconds() * 1.5f)));
        }

        // 如果会话中已存储这个新连接的clientId, 就关闭之前该clientId的连接
        if (sessionStoreService.containsKey(mqttMessage.payload().clientIdentifier())){
            SessionStore sessionStore = sessionStoreService.get(mqttMessage.payload().clientIdentifier());
            Channel previous = sessionStore.getChannel();
            boolean cleanSession = sessionStore.isCleanSession();
            if (cleanSession){
                sessionStoreService.remove(mqttMessage.payload().clientIdentifier());
                subscribeStoreService.removeForClient(mqttMessage.payload().clientIdentifier());
                dupPublishMessageStoreService.removeByClient(mqttMessage.payload().clientIdentifier());
                dupPubRelMessageStoreService.removeByClient(mqttMessage.payload().clientIdentifier());
            }
            previous.close();
        }

        //处理遗嘱信息
        SessionStore sessionStore = new SessionStore(mqttMessage.payload().clientIdentifier(), channel, mqttMessage.variableHeader().isCleanSession(), null);
        if (mqttMessage.variableHeader().isWillFlag()){
            MqttPublishMessage willMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.PUBLISH,false, MqttQoS.valueOf(mqttMessage.variableHeader().willQos()),mqttMessage.variableHeader().isWillRetain(),0),
                    new MqttPublishVariableHeader(mqttMessage.payload().willTopic(),0),
                    Unpooled.buffer().writeBytes(mqttMessage.payload().willMessageInBytes())
            );
            sessionStore.setWillMessage(willMessage);
        }
        //至此存储会话消息
        sessionStoreService.put(mqttMessage.payload().clientIdentifier(),sessionStore);

        //将clientId存储到channel的map中
        channel.attr(AttributeKey.valueOf("clientId")).set(mqttMessage.payload().clientIdentifier());
        // 返回接受客户端连接
        boolean sessionPresent = sessionStoreService.containsKey(mqttMessage.payload().clientIdentifier()) && !mqttMessage.variableHeader().isCleanSession();
        MqttConnAckMessage okResp = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK,false,MqttQoS.AT_MOST_ONCE,false,0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED,sessionPresent), null);
        channel.writeAndFlush(okResp);
        log.info("CONNECT - clientId: {}, cleanSession: {}", mqttMessage.payload().clientIdentifier(), mqttMessage.variableHeader().isCleanSession());

        // 如果cleanSession为0, 需要重发同一clientId存储的未完成的QoS1和QoS2的DUP消息
        if (!mqttMessage.variableHeader().isCleanSession()){
            List<DupPublishMessageStore> dupPublishMessageStoreList = dupPublishMessageStoreService.get(mqttMessage.payload().clientIdentifier());
            List<DupPubRelMessageStore> dupPubRelMessageStoreList = dupPubRelMessageStoreService.get(mqttMessage.payload().clientIdentifier());
            dupPublishMessageStoreList.forEach(dupPublishMessageStore -> {
                MqttPublishMessage publishMessage = (MqttPublishMessage)MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBLISH,true,MqttQoS.valueOf(dupPublishMessageStore.getMqttQoS()),false,0),
                        new MqttPublishVariableHeader(dupPublishMessageStore.getTopic(),dupPublishMessageStore.getMessageId()),
                        Unpooled.buffer().writeBytes(dupPublishMessageStore.getMessageBytes())
                );
                channel.writeAndFlush(publishMessage);
            });
            dupPubRelMessageStoreList.forEach(dupPubRelMessageStore -> {
                MqttMessage pubRelMessage = MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBREL,true,MqttQoS.AT_MOST_ONCE,false,0),
                        MqttMessageIdVariableHeader.from(dupPubRelMessageStore.getMessageId()),
                        null
                );
                channel.writeAndFlush(pubRelMessage);
            });
        }
    }
}