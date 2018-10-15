# shine-mq

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/top.arkstack/shine-mq/badge.svg)](https://search.maven.org/artifact/top.arkstack/shine-mq/)
[![Latest release](https://img.shields.io/github/release/7le/shine-mq.svg)](https://github.com/7le/shine-mq/releases/latest)

### 🐳 maven

```
<dependency>
    <groupId>top.arkstack</groupId>
    <artifactId>shine-mq</artifactId>
    <version>1.1.0-SNAPSHOT</version>
</dependency>
```

### 🐣 主要功能

* 封装mq的操作，方便使用
* 实现基于可靠消息服务的分布式事务 (coding...)

### 🌈 使用文档

目前兼容Direct和Topic模式，支持springboot的配置，具体可配置的参数如下：

```
    /**
     * {@link org.springframework.amqp.core.AcknowledgeMode}
     * <p>
     * 0 AUTO
     * 1 MANUAL
     * 2 NONE
     */
    private int acknowledgeMode = 1;

    /**
     * 每个消费者可能未完成的未确认消息的数量。
     */
    private Integer prefetchCount = null;

    /**
     * 为每个已配置队列创建的消费者数
     */
    private Integer consumersPerQueue = null;

    /**
     * 是否持久化，指是否保存到erlang自带得数据库mnesia中，即重启服务是否消失
     */
    private boolean durable = true;

    /**
     * 是否排外，指当前定义的队列是connection中的channel共享的，其他connection连接访问不到
     */
    private boolean exclusive = false;

    /**
     * 是否自动删除，指当connection.close时队列删除
     */
    private boolean autoDelete = false;

    /**
     * 是否初始化消息监听者， 若服务只是Producer则关闭
     */
    private boolean listenerEnable = false;
```

**rabbitmq**的配置复用spring的配置

```
spring:
  rabbitmq:
    host: 114.215.122.xxx
    username: xxxxx
    password: xxxxx
```

如果需要开启消费者的服务的话，设置**listener-enable**参数为**true**，默认为**false**，以yml举例如下：

```
shine:
  mq:
    rabbit:
      listener-enable: true
```

对于生产者，demo如下，``RabbitmqFactory``已经注入spring容器，可以直接通过``@Autowired``获得。

通过**rabbitmqFactory.add**可以实现动态增加生产者和消费者。

```
@Component
public class Producer {

    @Autowired
    RabbitmqFactory rabbitmqFactory;

    @PostConstruct
    public void pull() throws Exception {
        rabbitmqFactory.add("queue-test", "exchange-test", "yoyo", null);
        rabbitmqFactory.start();
        for (int i = 0; i < 50; i++) {
            rabbitmqFactory.getTemplate().send("queue-test", "exchange-test", "hello world "+i, "yoyo");
        }
        System.out.println("------------------------pull end-------------------------------");
    }
}
```

对于消费者，demo如下，``Processor``需要自己实现，这里写获得消息后的业务处理。

```
@Component
public class Consumer {

    @Autowired
    RabbitmqFactory rabbitmqFactory;

    @PostConstruct
    public void pull() throws Exception {
        rabbitmqFactory.add("queue-test", "exchange-test", "yoyo", new ProcessorTest(), null);
        rabbitmqFactory.start();
    }

    static class ProcessorTest extends BaseProcessor {
    
        @Override
        public Object process(Object msg, Message message, Channel channel) {
            System.out.println(" process: " + msg);
            try {
                TimeUnit.SECONDS.sleep(10);
                //如果选择了MANUAL模式 需要手动回执ack
                //channel.basicAck(message.getMessageProperties().getDeliveryTag(),false);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            return null;
        }
    }
}
```
