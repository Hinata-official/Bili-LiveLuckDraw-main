package com.bilibili.liveluckdraw.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.sql.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Flink作业：实时处理B站弹幕，关联用户画像，生成个性化应答
 */
public class DanmuProcessorJob {
    // 日志记录器
    private static final Logger LOG = LoggerFactory.getLogger(DanmuProcessorJob.class);

    // JSON解析器
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Kafka配置
    // 使用容器名称进行连接，Docker内部网络中可以通过容器名访问
    private static final String KAFKA_BROKERS = "kafka:29092";
    private static final String INPUT_TOPIC = "bili-danmu";
    private static final String OUTPUT_TOPIC = "vtuber-response";
    private static final String ERROR_TOPIC = "danmu-processing-errors";
    private static final String GROUP_ID = "flink-danmu-processor";

    // MySQL配置 - 在Docker环境中使用容器名作为主机名
    private static final String MYSQL_URL = "jdbc:mysql://mysql:3306/user_profile?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "password";

    // 定义侧输出标签用于处理异常数据
    private static final OutputTag<String> ERROR_TAG = new OutputTag<String>("danmu-errors") {};

    // 全局配置日志
    static {
        LOG.info("DanmuProcessorJob 初始化配置：");
        LOG.info("Kafka Brokers: {}", KAFKA_BROKERS);
        LOG.info("Input Topic: {}", INPUT_TOPIC);
        LOG.info("Output Topic: {}", OUTPUT_TOPIC);
        LOG.info("Error Topic: {}", ERROR_TOPIC);
        LOG.info("MySQL URL: {}", MYSQL_URL.replaceAll(":\\w+@", ":***@")); // 隐藏密码
    }

    public static void main(String[] args) throws Exception {
        LOG.info("开始初始化 Flink 作业...");

        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 启用检查点机制
        LOG.info("配置检查点机制：60秒间隔，精确一次语义");
        env.enableCheckpointing(60000); // 60秒一次检查点
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000);
        checkpointConfig.setCheckpointTimeout(120000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置状态后端
        LOG.info("设置状态后端：file:///tmp/flink-checkpoints");
        env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));

        // 设置并行度
        LOG.info("设置并行度：{}", 1);
        env.setParallelism(1);

        // 设置重启策略
        LOG.info("配置重启策略：最大3次重试，间隔10秒");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 重试间隔
        ));

        // 配置Kafka消费者
        LOG.info("配置Kafka消费者...");
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 从Kafka读取弹幕数据
        LOG.info("从Kafka主题 [{}] 读取数据...", INPUT_TOPIC);
        DataStream<String> danmuStream = env.addSource(
                new FlinkKafkaConsumer<>
                (
                        INPUT_TOPIC,
                        new SimpleStringSchema(),
                        consumerProps
                )
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps()
        ).uid("danmu-source"); // 添加唯一ID以支持状态恢复

        // 设置容错参数
        LOG.info("配置水印间隔：5秒");
        env.getConfig().setAutoWatermarkInterval(5000); // 每5秒生成一次水印

        // 1. 解析弹幕数据并处理异常
        LOG.info("开始配置数据处理流水线...");
        // 先对数据流进行keyBy操作，以便在ErrorHandlingProcessFunction中使用键控状态
        SingleOutputStreamOperator<DanmuData> parsedDanmuStream = danmuStream
                .keyBy(value -> {
                    // 为了分区的稳定性，我们可以使用一个简单的分区策略
                    // 由于这里还无法解析弹幕数据，我们使用固定的分区键
                    return "default-key"; // 在实际处理中可以根据需要选择合适的分区键
                })
                .process(new ErrorHandlingProcessFunction())
                .uid("error-handling")
                .filter(danmu -> danmu != null) // 过滤掉解析失败的数据
                .uid("danmu-filter");

        // 处理解析失败的弹幕数据，发送到错误主题
        LOG.info("配置错误数据处理，发送到主题 [{}]", ERROR_TOPIC);
        DataStream<String> errorStream = parsedDanmuStream.getSideOutput(ERROR_TAG);
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        
        // 确保错误消息是字符串类型，避免序列化异常
        DataStream<String> safeErrorStream = errorStream.map(errorMsg -> {
            if (errorMsg == null) {
                return "[NULL ERROR MESSAGE]";
            }
            return errorMsg.toString();
        }).uid("safe-error-mapping");
        
        safeErrorStream.addSink(new FlinkKafkaProducer<>(
                ERROR_TOPIC,
                new SimpleStringSchema(),
                producerProps
        )).uid("error-sink");

        LOG.info("错误流配置完成");

        // 2. 关联用户画像
        LOG.info("配置用户画像关联处理...");
        // 先对数据流进行keyBy操作，以便在UserProfileEnricher中使用键控状态
        DataStream<DanmuWithUserProfile> enrichedDanmuStream = parsedDanmuStream
                .keyBy(danmu -> danmu.getUid()) // 按用户ID分区
                .map(new UserProfileEnricher());

        // 3. 生成个性化应答
        LOG.info("配置个性化应答生成...");
        // 先对数据流进行keyBy操作，以便在ResponseGenerator中使用键控状态
        DataStream<String> responseStream = enrichedDanmuStream
                .keyBy(danmuWithProfile -> danmuWithProfile.getDanmu().getUid()) // 按用户ID分区
                .map(new ResponseGenerator())
                .filter(response -> response != null) // 过滤掉生成失败的应答
                .uid("response-filter");

        // 4. 输出到Kafka应答主题
        LOG.info("配置Kafka生产者...");
        // 创建一个新的Properties对象，避免与错误流的配置冲突
        Properties responseProducerProps = new Properties();
        responseProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        responseProducerProps.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        // 移除了显式的序列化器配置，因为我们会在构造函数中传入自定义的SafeStringSerializer
        responseProducerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3"); // 添加重试配置

        LOG.info("配置应答输出到主题 [{}]", OUTPUT_TOPIC);
        
        // 确保响应消息是字符串类型，添加额外的类型安全检查
        DataStream<String> safeResponseStream = responseStream.map(response -> {
            if (response == null) {
                return "{\"error\": \"NULL RESPONSE\"}";
            }
            // 确保返回的是有效的字符串
            String result = String.valueOf(response);
            LOG.debug("响应字符串转换结果: {}, 类型: {}", result, response.getClass().getName());
            return result;
        }).uid("safe-response-mapping");
        
        // 使用自定义的安全序列化器，确保能处理任何类型的输入
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                OUTPUT_TOPIC,
                new SafeStringSerializer(),
                responseProducerProps
        );
        
        safeResponseStream.addSink(kafkaProducer).uid("response-sink"); // 添加唯一ID以支持状态恢复
        
        // 执行作业
        LOG.info("Flink作业配置完成，准备启动执行...");
        LOG.info("作业名称: Bili Live Danmu Processor");
        env.execute("Bili Live Danmu Processor");
        LOG.info("Flink作业已启动");
    }

    /**
     * 安全字符串序列化器，能够处理任何类型的输入并将其安全地转换为字符串
     */
    public static class SafeStringSerializer implements SerializationSchema<String> {
        private static final Logger LOG = LoggerFactory.getLogger(SafeStringSerializer.class);
        private static final ObjectMapper mapper = new ObjectMapper();
        
        @Override
        public byte[] serialize(String element) {
            try {
                if (element == null) {
                    LOG.warn("尝试序列化null值，返回空字节数组");
                    return "{\"error\": \"NULL_VALUE\"}".getBytes(StandardCharsets.UTF_8);
                }
                
                LOG.debug("序列化元素，类型: {}, 长度: {}", element.getClass().getName(), element.length());
                // 确保返回的是UTF-8编码的字节数组
                return element.getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                LOG.error("序列化失败: {}", e.getMessage(), e);
                // 发生任何序列化错误时，返回错误信息的字节数组
                return String.format("{\"serialization_error\": \"%s\"}", 
                        escapeJson(e.getMessage())).getBytes(StandardCharsets.UTF_8);
            }
        }
        
        private String escapeJson(String input) {
            if (input == null) return "null";
            return input.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    /**
     * 错误处理ProcessFunction，用于捕获和处理解析过程中的异常
     */
    public static class ErrorHandlingProcessFunction extends ProcessFunction<String, DanmuData> {
        private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlingProcessFunction.class);
        private static final ObjectMapper mapper = new ObjectMapper();
        private transient ValueState<Long> errorCounterState;
        private static final long MAX_ERROR_THRESHOLD = 100; // 错误阈值
        private transient ValueState<Long> successCounterState;
        private long lastStatsTime = System.currentTimeMillis();
        private static final long STATS_INTERVAL = 60000; // 每分钟记录一次统计信息

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<Long> errorDescriptor = new ValueStateDescriptor<>("errorCounter", Long.class, 0L);
            errorCounterState = getRuntimeContext().getState(errorDescriptor);

            ValueStateDescriptor<Long> successDescriptor = new ValueStateDescriptor<>("successCounter", Long.class, 0L);
            successCounterState = getRuntimeContext().getState(successDescriptor);

            LOG.info("ErrorHandlingProcessFunction 初始化完成，错误阈值: {}", MAX_ERROR_THRESHOLD);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<DanmuData> out) throws Exception {
            try {
                // 定期记录处理统计信息
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastStatsTime >= STATS_INTERVAL) {
                    LOG.info("弹幕解析统计 - 成功: {}, 失败: {}",
                            successCounterState.value(), errorCounterState.value());
                    lastStatsTime = currentTime;
                }

                // 解析JSON格式的弹幕数据
                LOG.debug("接收到原始弹幕数据: {}", value);
                JsonNode rootNode = mapper.readTree(value);

                // 从JSON中提取所需字段
                long uid = rootNode.has("uid") ? rootNode.get("uid").asLong() : 0;
                // 同时检查username(全小写)、userName(驼峰)和name字段，优先使用username
                String username = rootNode.has("username") ? rootNode.get("username").asText() :
                                 (rootNode.has("userName") ? rootNode.get("userName").asText() : 
                                 (rootNode.has("name") ? rootNode.get("name").asText() : "未知用户"));
                String content = rootNode.has("content") ? rootNode.get("content").asText() : "";
                long roomId = rootNode.has("roomId") ? rootNode.get("roomId").asLong() : 0;

                // 确保必要字段存在
                if (uid > 0 && !content.isEmpty()) {
                    DanmuData danmuData = new DanmuData(uid, username, content, roomId);
                    out.collect(danmuData);

                    // 更新成功计数器
                    long successCount = successCounterState.value();
                    successCounterState.update(successCount + 1);

                    LOG.debug("成功解析弹幕 - 房间: {}, 用户: {}({}), 内容: {}",
                            roomId, username, uid, content);
                } else {
                    // 记录缺少必要字段的情况
                    long errorCount = errorCounterState.value();
                    errorCounterState.update(errorCount + 1);

                    String errorMsg = String.format("缺少必要字段 - UID: %d, 内容: '%s'", uid, content);
                    LOG.warn(errorMsg);

                    if (errorCount < MAX_ERROR_THRESHOLD) {
                        // 前MAX_ERROR_THRESHOLD条错误数据记录到侧输出流
                        ctx.output(ERROR_TAG, "缺少必要字段: " + value);
                    } else if (errorCount % 100 == 0) {
                        // 超过阈值后，每100条记录一次，避免日志爆炸
                        String batchMsg = "大量缺少必要字段的消息，当前计数: " + errorCount;
                        LOG.warn(batchMsg);
                        ctx.output(ERROR_TAG, batchMsg);
                    }
                }
            } catch (Exception e) {
                // 捕获解析异常
                long errorCount = errorCounterState.value();
                errorCounterState.update(errorCount + 1);

                String errorMsg = String.format("解析失败: %s, 错误: %s", value, e.getMessage());
                LOG.error(errorMsg, e);

                if (errorCount < MAX_ERROR_THRESHOLD) {
                    ctx.output(ERROR_TAG, errorMsg);
                } else if (errorCount % 100 == 0) {
                    String batchMsg = String.format("大量解析失败消息，当前计数: %d, 最近错误: %s",
                            errorCount, e.getMessage());
                    LOG.error(batchMsg);
                    ctx.output(ERROR_TAG, batchMsg);
                }
            }
        }
    }

    /**
     * 用户画像关联器
     */
    public static class UserProfileEnricher extends RichMapFunction<DanmuData, DanmuWithUserProfile> {
        private static final Logger LOG = LoggerFactory.getLogger(UserProfileEnricher.class);
        private transient Connection connection;
        private transient PreparedStatement preparedStatement;
        private transient ValueState<Tuple2<Long, Integer>> mysqlErrorState; // (lastErrorTime, errorCount)
        private static final int MAX_RETRIES = 3; // 最大重试次数
        private static final long RETRY_INTERVAL_MS = 1000; // 重试间隔
        private transient ValueState<Long> userProfileFoundState;
        private transient ValueState<Long> userProfileNotFoundState;
        private long lastStatsTime = System.currentTimeMillis();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 初始化MySQL连接池
            LOG.info("UserProfileEnricher 初始化开始...");
            initializeConnection();

            // 初始化状态
            ValueStateDescriptor<Tuple2<Long, Integer>> errorDescriptor =
                    new ValueStateDescriptor<>("mysqlErrorState", TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>() {}));
            mysqlErrorState = getRuntimeContext().getState(errorDescriptor);

            ValueStateDescriptor<Long> foundDescriptor =
                    new ValueStateDescriptor<>("userProfileFound", Long.class, 0L);
            userProfileFoundState = getRuntimeContext().getState(foundDescriptor);

            ValueStateDescriptor<Long> notFoundDescriptor =
                    new ValueStateDescriptor<>("userProfileNotFound", Long.class, 0L);
            userProfileNotFoundState = getRuntimeContext().getState(notFoundDescriptor);

            LOG.info("UserProfileEnricher 初始化完成，MySQL连接已建立");
        }

        private void initializeConnection() throws Exception {
            try {
                // 显式加载MySQL驱动类，确保在容器环境中正确加载
                LOG.info("正在加载MySQL驱动...");
                Class.forName("com.mysql.cj.jdbc.Driver");
                
                // 使用连接池属性
                LOG.info("正在初始化MySQL连接...");
                Properties props = new Properties();
                props.setProperty("user", MYSQL_USER);
                props.setProperty("password", MYSQL_PASSWORD);
                props.setProperty("autoReconnect", "true");
                props.setProperty("maxReconnects", "3");
                props.setProperty("useSSL", "false");
                props.setProperty("serverTimezone", "UTC");

                connection = DriverManager.getConnection(MYSQL_URL, props);
                String query = "SELECT * FROM user_profile WHERE uid = ?";
                preparedStatement = connection.prepareStatement(query);

                LOG.info("MySQL连接成功，URL: {}", MYSQL_URL.replaceAll(":\\w+@", ":***@"));
            } catch (Exception e) {
                LOG.error("初始化MySQL连接失败: {}", e.getMessage(), e);
                throw e;
            }
        }

        @Override
        public DanmuWithUserProfile map(DanmuData danmu) throws Exception {
            if (danmu == null) {
                LOG.warn("接收到空的弹幕数据，跳过处理");
                return null;
            }

            long uid = danmu.getUid();
            String username = danmu.getUsername();
            LOG.debug("处理用户弹幕 - UID: {}, 用户名: {}", uid, username);

            // 定期记录统计信息
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastStatsTime >= 60000) { // 每分钟记录一次
                LOG.info("用户画像查询统计 - 找到: {}, 未找到: {}",
                        userProfileFoundState.value(), userProfileNotFoundState.value());
                lastStatsTime = currentTime;
            }

            int retries = 0;
            while (retries < MAX_RETRIES) {
                try {
                    // 检查连接是否有效
                    if (connection == null || connection.isClosed() || !connection.isValid(1)) {
                        LOG.info("MySQL连接已关闭或无效，重新初始化连接...");
                        initializeConnection();
                    }

                    // 从MySQL查询用户画像
                    preparedStatement.setLong(1, uid);
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        UserProfile profile = null;
                        if (resultSet.next()) {
                            userProfileFoundState.update(userProfileFoundState.value() + 1);

                            String gender = resultSet.getString("gender");
                            int age = resultSet.getInt("age");
                            String avatar = resultSet.getString("avatar");
                            String[] tags = resultSet.getString("tags").split(",");

                            profile = new UserProfile(uid, gender, age, avatar, tags);
                            LOG.debug("成功找到用户画像 - UID: {}, 性别: {}, 年龄: {}, 标签数量: {}",
                                    uid, gender, age, tags.length);
                        } else {
                            userProfileNotFoundState.update(userProfileNotFoundState.value() + 1);

                            // 如果用户画像不存在，返回默认值
                            profile = new UserProfile(uid, "unknown", 0, "", new String[0]);
                            LOG.debug("未找到用户画像，使用默认值 - UID: {}", uid);
                        }

                        // 重置错误状态
                        if (mysqlErrorState.value() != null) {
                            LOG.debug("重置MySQL错误状态");
                            mysqlErrorState.clear();
                        }

                        // 返回关联后的弹幕数据
                        DanmuWithUserProfile result = new DanmuWithUserProfile(danmu, profile);
                        LOG.debug("用户画像关联完成 - UID: {}", uid);
                        return result;
                    }
                } catch (Exception e) {
                    retries++;

                    // 更新错误状态
                    Tuple2<Long, Integer> errorState = mysqlErrorState.value();
                    if (errorState == null) {
                        errorState = new Tuple2<>(System.currentTimeMillis(), 1);
                    } else {
                        errorState.f0 = System.currentTimeMillis();
                        errorState.f1 += 1;
                    }
                    mysqlErrorState.update(errorState);

                    LOG.warn("查询用户画像失败，正在重试 ({}次/{}次): {}",
                            retries, MAX_RETRIES, e.getMessage());

                    if (retries >= MAX_RETRIES) {
                        LOG.error("查询用户画像失败，已达到最大重试次数: {}", e.getMessage(), e);

                        // 记录错误但继续处理，返回仅包含弹幕的对象
                        return new DanmuWithUserProfile(danmu, null);
                    }

                    // 等待一段时间后重试
                    long sleepTime = RETRY_INTERVAL_MS * retries;
                    LOG.debug("等待{}ms后重试...", sleepTime);
                    Thread.sleep(sleepTime); // 指数退避
                }
            }

            // 理论上不会到达这里，但为了安全起见
            LOG.warn("处理流程异常结束，返回默认结果");
            return new DanmuWithUserProfile(danmu, null);
        }

        @Override
        public void close() throws Exception {
            LOG.info("UserProfileEnricher 关闭资源...");
            // 关闭MySQL连接和PreparedStatement
            if (preparedStatement != null && !preparedStatement.isClosed()) {
                preparedStatement.close();
                LOG.info("PreparedStatement已关闭");
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
                LOG.info("MySQL连接已关闭");
            }
        }
    }

    /**
     * 个性化应答生成器
     */
    public static class ResponseGenerator extends RichMapFunction<DanmuWithUserProfile, String> {
        private static final Logger LOG = LoggerFactory.getLogger(ResponseGenerator.class);
        private transient ValueState<Long> responseCounterState; // 记录生成的应答数量
        private transient ValueState<Map<String, Integer>> keywordStatsState; // 记录关键词统计
        private transient ValueState<Long> responseSuccessState;
        private transient ValueState<Long> responseErrorState;
        private long lastStatsTime = System.currentTimeMillis();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            LOG.info("ResponseGenerator 初始化开始...");

            // 初始化状态
            ValueStateDescriptor<Long> counterDescriptor =
                    new ValueStateDescriptor<>("responseCounter", Long.class, 0L);
            responseCounterState = getRuntimeContext().getState(counterDescriptor);

            ValueStateDescriptor<Map<String, Integer>> statsDescriptor =
                    new ValueStateDescriptor<>("keywordStats", TypeInformation.of(new TypeHint<Map<String, Integer>>() {}));
            keywordStatsState = getRuntimeContext().getState(statsDescriptor);

            ValueStateDescriptor<Long> successDescriptor =
                    new ValueStateDescriptor<>("responseSuccess", Long.class, 0L);
            responseSuccessState = getRuntimeContext().getState(successDescriptor);

            ValueStateDescriptor<Long> errorDescriptor =
                    new ValueStateDescriptor<>("responseError", Long.class, 0L);
            responseErrorState = getRuntimeContext().getState(errorDescriptor);

            LOG.info("ResponseGenerator 初始化完成");
        }

        @Override
        public String map(DanmuWithUserProfile enrichedDanmu) throws Exception {
            if (enrichedDanmu == null || enrichedDanmu.getDanmu() == null) {
                LOG.warn("接收到空的弹幕或用户画像数据，跳过处理");
                return null;
            }

            DanmuData danmu = enrichedDanmu.getDanmu();
            UserProfile profile = enrichedDanmu.getProfile();

            long uid = danmu.getUid();
            String username = danmu.getUsername();
            String content = danmu.getContent();
            long roomId = danmu.getRoomId();

            try {
                // 更新应答计数器
                long counter = responseCounterState.value();
                responseCounterState.update(counter + 1);

                // 更新关键词统计
                updateKeywordStats(content);

                // 定期打印统计信息
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastStatsTime >= 60000) { // 每分钟记录一次
                    LOG.info("应答生成统计 - 总数量: {}, 成功: {}, 失败: {}",
                            counter, responseSuccessState.value(), responseErrorState.value());

                    // 记录关键词统计信息
                    Map<String, Integer> stats = keywordStatsState.value();
                    if (stats != null && !stats.isEmpty()) {
                        StringBuilder statsMsg = new StringBuilder("关键词统计: ");
                        for (Map.Entry<String, Integer> entry : stats.entrySet()) {
                            statsMsg.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
                        }
                        LOG.info(statsMsg.substring(0, statsMsg.length() - 2));
                    }

                    lastStatsTime = currentTime;
                }

                // 生成个性化应答
                LOG.debug("为用户生成个性化应答 - UID: {}, 用户名: {}, 内容: {}",
                        uid, username, content);
                String responseContent = generatePersonalizedResponse(danmu, profile);

                // 使用Jackson构造JSON，避免手动拼接带来的问题
                JsonNodeFactory factory = JsonNodeFactory.instance;
                ObjectNode responseNode = factory.objectNode();
                responseNode.put("userId", uid);
                responseNode.put("userName", username);
                responseNode.put("userAvatar", getAvatarUrl(uid));
                // 同时添加response和responseContent字段，确保与VtuberResponseConsumer兼容
                responseNode.put("responseContent", responseContent);
                responseNode.put("response", responseContent);
                responseNode.put("timestamp", System.currentTimeMillis());
                responseNode.put("roomId", roomId);
                responseNode.put("originalMessage", content);
                responseNode.put("confidence", 0.9);
                // 添加标记，区分来自Flink的响应消息
                responseNode.put("fromFlink", true);

                // 添加用户画像相关信息（如果存在）
                if (profile != null) {
                    LOG.debug("添加用户画像信息到应答 - UID: {}, 性别: {}, 年龄: {}",
                            uid, profile.getGender(), profile.getAge());
                    ObjectNode profileNode = factory.objectNode();
                    profileNode.put("gender", profile.getGender());
                    profileNode.put("age", profile.getAge());
                    profileNode.put("avatar", profile.getAvatar());
                    responseNode.set("userProfile", profileNode);
                }

                String responseJson = objectMapper.writeValueAsString(responseNode);
                responseSuccessState.update(responseSuccessState.value() + 1);

                LOG.debug("成功生成应答 - UID: {}, 内容: {}",
                        uid, responseContent);
                return responseJson;
            } catch (Exception e) {
                responseErrorState.update(responseErrorState.value() + 1);
                LOG.error("生成应答失败 - UID: {}, 用户名: {}, 内容: {}: {}",
                        uid, username, content, e.getMessage(), e);

                // 发生异常时，返回默认应答
                String defaultResponse = String.format(
                        "{\"userId\": %d, \"userName\": \"%s\", \"responseContent\": \"%s\", \"response\": \"%s\", \"timestamp\": %d}",
                        uid,
                        escapeJson(username),
                        escapeJson("抱歉，我现在有点忙，稍后回复你~"),
                        escapeJson("抱歉，我现在有点忙，稍后回复你~"),
                        System.currentTimeMillis()
                );
                LOG.debug("返回默认应答: {}", defaultResponse);
                return defaultResponse;
            }
        }

        private void updateKeywordStats(String content) throws Exception {
            Map<String, Integer> stats = keywordStatsState.value();
            if (stats == null) {
                stats = new HashMap<>();
            }

            // 简单的关键词提取示例
            String[] keywords = {"抽奖", "游戏", "主播", "666", "你好", "再见"};
            List<String> foundKeywords = new ArrayList<>();

            for (String keyword : keywords) {
                if (content.contains(keyword)) {
                    int count = stats.getOrDefault(keyword, 0) + 1;
                    stats.put(keyword, count);
                    foundKeywords.add(keyword);
                }
            }

            if (!foundKeywords.isEmpty()) {
                keywordStatsState.update(stats);
                LOG.debug("更新关键词统计 - 找到: {}, 当前统计: {}",
                        String.join(", ", foundKeywords), stats);
            }
        }

        private String getAvatarUrl(long uid) {
            // 示例：生成用户头像URL
            String url = String.format("https://i0.hdslb.com/bfs/face/%s.jpg", uid);
            LOG.debug("生成用户头像URL: {}", url);
            return url;
        }

        private String generatePersonalizedResponse(DanmuData danmu, UserProfile profile) {
            try {
                // 基于用户画像和弹幕内容生成个性化应答
                String content = danmu.getContent().toLowerCase();
                String username = danmu.getUsername();

                // 1. 优先检查基于内容的关键词匹配
                if (content.contains("hello") || content.contains("你好")) {
                    String response = String.format("你好呀，%s！很高兴见到你~", username);
                    LOG.debug("基于关键词生成应答: {}", response);
                    return response;
                } else if (content.contains("抽奖")) {
                    String response = "抽奖活动正在进行中，请持续关注哦！";
                    LOG.debug("基于关键词生成应答: {}", response);
                    return response;
                } else if (content.contains("主播")) {
                    String response = "谢谢支持~我会继续努力的！";
                    LOG.debug("基于关键词生成应答: {}", response);
                    return response;
                } else if (content.contains("666") || content.contains("牛批") || content.contains("厉害")) {
                    String response = "谢谢夸奖，这都是我应该做的~";
                    LOG.debug("基于关键词生成应答: {}", response);
                    return response;
                } else if (content.contains("再见") || content.contains("拜拜")) {
                    String response = String.format("下次见啦，%s！不要忘记来看我哦~", username);
                    LOG.debug("基于关键词生成应答: {}", response);
                    return response;
                }

                // 2. 基于用户画像生成个性化应答
                if (profile != null) {
                    LOG.debug("使用用户画像生成个性化应答");
                    // 检查用户标签
                    if (profile.getTags() != null && profile.getTags().length > 0) {
                        // 检查用户是否有游戏标签
                        boolean hasGameTag = false;
                        for (String tag : profile.getTags()) {
                            if (tag.contains("游戏") || tag.contains("电竞")) {
                                hasGameTag = true;
                                break;
                            }
                        }

                        if (hasGameTag && content.contains("游戏")) {
                            String response = String.format("%s也喜欢玩游戏吗？我们可以一起交流哦！", username);
                            LOG.debug("基于用户标签生成应答: {}", response);
                            return response;
                        }

                        // 检查用户是否有动漫标签
                        boolean hasAnimeTag = false;
                        for (String tag : profile.getTags()) {
                            if (tag.contains("动漫") || tag.contains("二次元")) {
                                hasAnimeTag = true;
                                break;
                            }
                        }

                        if (hasAnimeTag && content.contains("动漫")) {
                            String response = String.format("%s也喜欢看动漫吗？最近有什么好看的推荐吗？", username);
                            LOG.debug("基于用户标签生成应答: {}", response);
                            return response;
                        }
                    }
                }

                // 3. 默认通用应答
                String[] defaultResponses = {
                        String.format("感谢你的弹幕，%s！", username),
                        String.format("%s说得对！我也是这么想的~", username),
                        String.format("哈哈，%s真有趣！", username),
                        String.format("收到%s的消息啦，谢谢支持~", username),
                        "谢谢你的互动，我会继续努力的！"
                };

                // 随机选择一个默认回复，增加多样性
                int randomIndex = (int) (Math.random() * defaultResponses.length);
                String response = defaultResponses[randomIndex];
                LOG.debug("使用随机默认应答: {}", response);
                return response;
            } catch (Exception e) {
                LOG.error("生成个性化应答时发生错误: {}", e.getMessage(), e);
                return "感谢你的互动！";
            }
        }

        private String escapeJson(String input) {
            return input.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }

    // 数据模型类
    public static class DanmuData {
        private long uid;
        private String username;
        private String content;
        private long roomId;

        public DanmuData(long uid, String username, String content, long roomId) {
            this.uid = uid;
            this.username = username;
            this.content = content;
            this.roomId = roomId;
        }

        public long getUid() { return uid; }
        public String getUsername() { return username; }
        public String getContent() { return content; }
        public long getRoomId() { return roomId; }
    }

    public static class UserProfile {
        private long uid;
        private String gender;
        private int age;
        private String avatar;
        private String[] tags;

        public UserProfile(long uid, String gender, int age, String avatar, String[] tags) {
            this.uid = uid;
            this.gender = gender;
            this.age = age;
            this.avatar = avatar;
            this.tags = tags;
        }

        public long getUid() { return uid; }
        public String getGender() { return gender; }
        public int getAge() { return age; }
        public String getAvatar() { return avatar; }
        public String[] getTags() { return tags; }
    }

    public static class DanmuWithUserProfile {
        private DanmuData danmu;
        private UserProfile profile;

        public DanmuWithUserProfile(DanmuData danmu, UserProfile profile) {
            this.danmu = danmu;
            this.profile = profile;
        }

        public DanmuData getDanmu() { return danmu; }
        public UserProfile getProfile() { return profile; }
    }
}