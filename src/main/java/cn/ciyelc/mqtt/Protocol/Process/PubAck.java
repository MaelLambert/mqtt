package cn.ciyelc.mqtt.Protocol.Process;
import cn.ciyelc.mqtt.server.DupPublishMessageStoreService;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.util.AttributeKey;

public class PubAck {
    private DupPublishMessageStoreService dupPublishMessageStoreService;

    public PubAck(DupPublishMessageStoreService dupPublishMessageStoreService){
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
    }

    public void processPubAck(Channel channel, MqttMessageIdVariableHeader variableHeader){
        int messageId = variableHeader.messageId();
        dupPublishMessageStoreService.remove((String) channel.attr(AttributeKey.valueOf("clientId")).get(), messageId);
    }
}
