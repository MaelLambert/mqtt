package cn.ciyelc.mqtt.config;
import cn.ciyelc.mqtt.Protocol.ProtocolProcess;
import cn.ciyelc.mqtt.model.SessionStore;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 *  MQTT处理器
 */
public class MqttTransportHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private ProtocolProcess protocolProcess;

    MqttTransportHandler(ProtocolProcess protocolProcess) {
        this.protocolProcess = protocolProcess;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MqttMessage mqttMessage) throws Exception {
        if (mqttMessage.decoderResult().isFailure()) {
            Throwable cause = mqttMessage.decoderResult().cause();
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                channelHandlerContext.writeAndFlush(MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false),
                        null));
            } else if (cause instanceof MqttIdentifierRejectedException) {
                channelHandlerContext.writeAndFlush(MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false),
                        null));
            }
            channelHandlerContext.close();
            return;
        }
        switch (mqttMessage.fixedHeader().messageType()) {
            case CONNECT:
                protocolProcess.connect().processConnect(channelHandlerContext.channel(), (MqttConnectMessage) mqttMessage);
                break;
            case DISCONNECT:
                protocolProcess.disConnect().processDisConnect(channelHandlerContext.channel());
                break;
            case CONNACK:
                break;
            case PUBLISH:
                protocolProcess.publish().processPublish(channelHandlerContext.channel(), (MqttPublishMessage) mqttMessage);
                break;
            case PUBACK:
                protocolProcess.pubAck().processPubAck(channelHandlerContext.channel(), (MqttMessageIdVariableHeader) mqttMessage.variableHeader());
                break;
            case PUBREC:
                protocolProcess.pubRec().processPubRec(channelHandlerContext.channel(), (MqttMessageIdVariableHeader) mqttMessage.variableHeader());
                break;
            case PUBREL:
                protocolProcess.pubRel().processPubRel(channelHandlerContext.channel(), (MqttMessageIdVariableHeader) mqttMessage.variableHeader());
                break;
            case PUBCOMP:
                protocolProcess.pubComp().processPubComp(channelHandlerContext.channel(), (MqttMessageIdVariableHeader) mqttMessage.variableHeader());
                break;
            case SUBSCRIBE:
                protocolProcess.subscribe().processSubscribe(channelHandlerContext.channel(), (MqttSubscribeMessage) mqttMessage);
                break;
            case SUBACK:
                break;
            case UNSUBSCRIBE:
                protocolProcess.unSubscribe().processUnSubscribe(channelHandlerContext.channel(), (MqttUnsubscribeMessage) mqttMessage);
                break;
            case UNSUBACK:
                break;
            case PINGREQ:
                protocolProcess.pingReq().processPingReq(channelHandlerContext.channel());
                break;
            case PINGRESP:
                break;
            default:
                break;
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            // 远程主机强迫关闭了一个现有的连接的异常
            ctx.close();
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
            if (idleStateEvent.state() == IdleState.ALL_IDLE) {
                Channel channel = ctx.channel();
                String clientId = (String) channel.attr(AttributeKey.valueOf("clientId")).get();
                // 发送遗嘱消息
                if (this.protocolProcess.getSessionStoreService().containsKey(clientId)) {
                    SessionStore sessionStore = this.protocolProcess.getSessionStoreService().get(clientId);
                    if (sessionStore.getWillMessage() != null) {
                        this.protocolProcess.publish().processPublish(ctx.channel(), sessionStore.getWillMessage());
                    }
                }
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
