package top.arkstack.shine.mq;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import top.arkstack.shine.mq.bean.EventMessage;
import top.arkstack.shine.mq.bean.SendTypeEnum;
import top.arkstack.shine.mq.constant.MqConstant;
import top.arkstack.shine.mq.coordinator.Coordinator;
import top.arkstack.shine.mq.processor.Processor;
import top.arkstack.shine.mq.template.RabbitmqTemplate;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 消息适配处理器
 *
 * @author 7le
 * @version 1.0.0
 */
@Slf4j
@Component
public class MessageAdapterHandler implements ChannelAwareMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageAdapterHandler.class);

    /**
     * 缓存每个队列与对应的消费的接口的绑定关系，以及每个队列中的JSON数据与Object对象的之间转换关系（发送到同队列中的数据是同一种Object对象）
     */
    private ConcurrentMap<String, ProcessorWrap> map;

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    RabbitmqFactory rabbitmqFactory;

    protected MessageAdapterHandler() {
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * RabbitmqFactory落地添加、删除队列和交换机，而MessageAdapterHandler中的add、remove存储exchange-queue关系。
     * 减少队列、交换机在rabbit的broker中不必要的再次声明（减少web系统与rabbit之间网络交互时间），如在生产者中已经生成交换机与队列，消费者端就不必要再次声明，虽然broker中存在队列、交换机会直接返回
     * @param exchangeName
     * @param routingKey
     * @param processor
     * @param type
     * @param messageConverter
     */
    protected void add(String exchangeName, String routingKey, Processor processor, SendTypeEnum type,
                       MessageConverter messageConverter) {

        Objects.requireNonNull(exchangeName, "The exchangeName is empty.");
        Objects.requireNonNull(messageConverter, "The messageConverter is empty.");
        Objects.requireNonNull(routingKey, "The routingKey is empty.");

        ProcessorWrap pw = new ProcessorWrap(messageConverter, processor);
        ProcessorWrap oldProcessorWrap = map.putIfAbsent(exchangeName + "_" + routingKey + "_" +
                (type == null ? SendTypeEnum.DIRECT.toString() : type.toString()), pw);
        if (oldProcessorWrap != null) {
            logger.warn("The processor of this queue and exchange exists");
        }
    }

    protected void remove(String exchangeName, String routingKey, SendTypeEnum type) {
        map.remove(exchangeName + "_" + routingKey + "_" +
                (type == null ? SendTypeEnum.DIRECT.toString() : type.toString()));
    }

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        EventMessage em = null;
        Coordinator coordinator = null;
        long tag = message.getMessageProperties().getDeliveryTag();
        String msgId = message.getMessageProperties().getMessageId();
        try {
            ProcessorWrap wrap;
            // TODO 阿里fastJson存在漏洞，需要替换掉JSON框架
            em = JSON.parseObject(message.getBody(), EventMessage.class);
            if (MqConstant.DEAD_LETTER_EXCHANGE.equals(message.getMessageProperties().getReceivedExchange()) &&
                    MqConstant.DEAD_LETTER_ROUTEKEY.equals(message.getMessageProperties().getReceivedRoutingKey())) {
                wrap = map.get(MqConstant.DEAD_LETTER_EXCHANGE + "_" + MqConstant.DEAD_LETTER_ROUTEKEY +
                        "_" + SendTypeEnum.DLX);
            } else {
                wrap = map.get(em.getExchangeName() + "_" + em.getRoutingKey() + "_" + em.getSendTypeEnum());
            }
            //如果是分布式事务的消息，sdk提供ack应答，无须自己手动ack
            if (SendTypeEnum.DISTRIBUTED.toString().equals(em.getSendTypeEnum())) {
                Objects.requireNonNull(em.getCoordinator(),
                        "Distributed transaction message error: coordinator is null.");
                coordinator = (Coordinator) applicationContext.getBean(em.getCoordinator());
                wrap.process(em.getData(), message, channel);
                channel.basicAck(tag, false);
            } else {
                wrap.process(em.getData(), message, channel);
            }
        } catch (Exception e) {
            log.error("MessageAdapterHandler error, message: {} :", em, e);
            if (em != null && coordinator != null && SendTypeEnum.DISTRIBUTED.toString().equals(em.getSendTypeEnum())) {
                Double resendCount = coordinator.incrementResendKey(MqConstant.RECEIVE_RETRIES, msgId);
                if (resendCount >= rabbitmqFactory.getConfig().getDistributed().getReceiveMaxRetries()) {
                    if (Strings.isNullOrEmpty(em.getRollback())) {
                        // 放入死信队列
                        channel.basicNack(tag, false, false);
                    } else {
                        em.setSendTypeEnum(SendTypeEnum.ROLLBACK.toString());
                        em.setRoutingKey(em.getRollback());
                        rabbitmqFactory.setCorrelationData(msgId, em.getCoordinator(), em,
                                null);
                        coordinator.setRollback(msgId, em);
                        rabbitmqFactory.getTemplate().send(em, 0, 0, SendTypeEnum.ROLLBACK);
                        channel.basicAck(tag, false);
                    }
                    coordinator.delResendKey(MqConstant.RECEIVE_RETRIES, msgId);
                } else {
                    // 重新放入队列 等待消费
                    channel.basicNack(tag, false, true);
                }
            } else {
                throw e;
            }
        }
    }


    protected Set<String> getAllBinding() {
        Set<String> keySet = map.keySet();
        return keySet;
    }

    protected static class ProcessorWrap {

        // springboot-rabbitMQ内置的消息转换器
        private MessageConverter messageConverter;

        // 消费接口
        private Processor processor;

        protected ProcessorWrap(MessageConverter messageConverter, Processor processor) {
            this.messageConverter = messageConverter;
            this.processor = processor;
        }

        public Object process(Object msg, Message message, Channel channel) {
            return processor.process(msg, message, channel);
        }
    }
}
