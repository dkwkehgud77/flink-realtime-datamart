import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

public class MainStream {

    private static final Logger logger = LoggerFactory.getLogger(MainStream.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");

    private static final String CONSUMER_GROUP = RESOURCE_BUNDLE.getString("kafka.group");

    public static class KafkaToRows implements FlatMapFunction<String, Row>{
        // FlatMapFunction 인터페이스를 상속받아서, Kafka로 부터 수집한 메시지를 ETL 작업을 수행한 결과를 Row 데이터로 반환하는 함수를 구현합니다.

        public static MySQLConnectionPool dbPool = null;
        // MySQLConnectionPool 객체가 한번만 생성이 되도록 static 변수에 저장하여 객체를 메모리에 올라가도록 합니다.

        public Connection getConnMySQL() throws SQLException {
            Connection conn = null;
            if(dbPool == null){
                try{
                    // FlatMapFunction 안에서 MySQL 드라이버를 찾기위해 아래의 로직이 필요합니다.
                    Class.forName("com.mysql.cj.jdbc.Driver");
                } catch (ClassNotFoundException e) {
                    logger.error(" !! JDBC Driver load 오류: " + e.getMessage());
                    e.printStackTrace();
                }
                // MySQLConnectionPool 객체 생성
                dbPool = new MySQLConnectionPool(
                        RESOURCE_BUNDLE.getString("aurora.url")+"/Hlms?useSSL=false",
                        RESOURCE_BUNDLE.getString("aurora.username"),
                        RESOURCE_BUNDLE.getString("aurora.password"),70);
            }
            try {
                // MySQLConnectionPool 에서 커넥션 pool을 가져옵니다.
                conn = dbPool.getConnection();
                logger.info("occupiedPool size : " + String.valueOf(dbPool.occupiedPool.size()));
                logger.info("freePool size : " + String.valueOf(dbPool.freePool.size()));

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            return conn;
        }

        @Override
        public void flatMap(String s, Collector<Row> rows) throws Exception {
            // FlatMapFunction 인터페이스를 상속받아서, Kafka 로 부터 수집한 메시지를 ETL 작업을 수행한 결과를 Row 데이터로 반환하는 함수를 구현합니다.
            Gson gson = new Gson();
//            Thread.sleep(100);
            // Kafka CDC 로그 메시지를 Json 포맷으로 변환시킵니다.
            try {
                if (s != null && s.length() > 0) {
                    Map<String, Object> data = gson.fromJson(s, Map.class);
                    Map<String, Object> payload = (Map<String, Object>) data.get("payload");
//            logger.info(payload.toString());
                    String table = payload.get("__table").toString();
                    String source_ts_ms = payload.get("__source_ts_ms").toString();
                    String Learning_Seq = null;
                    // TBL_ENROLL, TBL_LEARNING_SUBJECT 토픽의 CDC 로그 데이터 분기 처리 --> Learning_Seq 값 구합니다.
                    // Learning_Seq는 데이터 마트 TBL_LEARNING_SUBJECT_PROGRESS_RATIO 테이블의 pk값 입니다.
                    if ("TBL_ENROLL".equals(table)) {
                        // 카프카 CDC 로그 데이터(Json)에서 Enroll_Seq, Confirm_Status_Cd 데이터를 추출합니다.
                        // TBL_ENROLL_MART

                        String Enroll_Seq = String.valueOf((int) Double.parseDouble(payload.get("Enroll_Seq").toString()));
                        Row row_case1 = Row.ofKind(RowKind.INSERT, Integer.parseInt(Enroll_Seq), "TBL_ENROLL_MART_TEST");
                        logger.info(table + " [" + source_ts_ms + "] " + CONSUMER_GROUP + " -> " + row_case1);
                        rows.collect(row_case1);

                        Connection conn = getConnMySQL();
                        try (Statement stmt = conn.createStatement()) {
                            String sql = "select Learning_Seq from Hlms.TBL_LEARNING where Enroll_Seq = " + Enroll_Seq;
                            ResultSet rs = stmt.executeQuery(sql);
                            while (rs.next()) {
                                // Enroll_Seq 을 기반으로 TBL_ENROLL 테이블에서  Learning_Seq 데이터를 구합니다.
                                Learning_Seq = rs.getString(1);
                                Row row_case2 = Row.ofKind(RowKind.INSERT, Integer.parseInt(Learning_Seq), "TBL_LEARNING_SUBJECT_PROGRESS_RATIO_JAVA");

                                logger.info(table + " [" + source_ts_ms + "] " + CONSUMER_GROUP + " -> " + row_case2);
                                rows.collect(row_case2);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("exception msg", e);
                        } finally {
                            if (conn != null) {
                                dbPool.returnConnection(conn);
                                // 작업이 끝나면 MySQLConnectionPool 에 커넥션 pool을 반환합니다.
                            }
                        }

                    } else if ("TBL_LEARNING_SUBJECT".equals(table)) {
                        // 카프카 CDC 로그 데이터(Json)에서 Learning_Seq 데이터를 추출합니다.
                        Learning_Seq = String.valueOf((int) Double.parseDouble(payload.get("Learning_Seq").toString()));
                        Row row_case1 = Row.ofKind(RowKind.INSERT, Integer.parseInt(Learning_Seq), "TBL_LEARNING_SUBJECT_PROGRESS_RATIO_JAVA");
                        logger.info(table + " [" + source_ts_ms + "] " + CONSUMER_GROUP + " -> " + row_case1);
                        rows.collect(row_case1);
                    }
                }
            } catch (Exception e){
                logger.error(e.getMessage());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        logger.info("Flink Job <LABS DATAMART> Start! ver 1.0");
        // StreamExecutionEnvironment 는 스트리밍 프로그램이 실행되는 컨텍스트이고, 작업 실행을 제어합니다. (Data Access, Fault Tolerance, Check Point )
        // StreamTableEnvironment는 DataStream API와 통합되는 Table 및 SQL API 프로그램을 만들기 위한 시작점이자 중앙 컨텍스트입니다.
        // DataStream --> Table 변환, Table --> DataStream 변환, 외부 시스템 연결 및 SQL 실행, 경계 없는 데이터 처리를 지원합니다.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointStorage("file:///home/hunetailab/flink-checkpoints/");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment t_env = StreamTableEnvironment.create(env,settings);

        // Data Mart ETL 쿼리에 필요한 Table들이 Flink 서버 JVM 메모리에 static 객체로 미리 생성합니다.
        ORMCreateTable.create_table(t_env);

        // Kafka Source를 구성. 카프카 BootstrapServer,Topics,Groupid 값 세팅, 메시지값만 역직렬화, 특정 시점(5분전)을 시작 Offset으로 설정합니다.
        List topics = Arrays.asList(RESOURCE_BUNDLE.getString("kafka.topics").split(",")) ;


        KafkaSource<String> source = null;
        DataStream<String> ds_kafka = null;
        DataStream<Row> ds_processed = null;

        try{
            source = KafkaSource.<String>builder()
                    .setBootstrapServers(RESOURCE_BUNDLE.getString("kafka.bootstrap.servers"))
//                    .setTopics("test-topic")
                    .setPartitions(new HashSet<>(Arrays.asList(new TopicPartition("test-topic",0))))
//                    .setPartitions(new HashSet<>(Arrays.asList(new TopicPartition("test-topic",1))))
//                    .setPartitions(new HashSet<>(Arrays.asList(new TopicPartition("test-topic",2))))
                    .setGroupId(RESOURCE_BUNDLE.getString("kafka.group"))
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setClientIdPrefix(UUID.randomUUID().toString())
                    .setStartingOffsets(OffsetsInitializer.latest()).build();     // 마지막 메시지 Offset 기준으로 수집

            // StreamExecutionEnvironment에서 Kafka Source으로 부터 데이터를 실시간으로 데이터를 수집하는 Data Stream을 생성합니다.
            ds_kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

            // Kafka DataStream에서 flatMap 변환을 통해 ETL 기능을 적용한 새로운 Data Stream을 생성합니다.
            // 데이터 스트림의 각 요소에 대해 flatMap 변환이 적용되고, FlatMapFunction 호출은 임의의 많은 결과 요소(없음 포함)을 반환할 수 있습니다.
            ds_processed = ds_kafka.flatMap(new KafkaToRows());
        } catch (Exception e){
            logger.error(e.getMessage());
        }

        if(ds_processed != null){
            final OutputTag<Row> outputTag1 = new OutputTag<Row>("TBL_LEARNING_SUBJECT_PROGRESS_RATIO_JAVA"){};
            String[] outputTag1_colums = {"Learning_Seq", "Target_Table"};
            TypeInformation<?>[] outputTag1_types = {Types.INT, Types.STRING};
            final OutputTag<Row> outputTag2 = new OutputTag<Row>("TBL_ENROLL_MART_TEST"){};
            String[] outputTag2_colums = {"Enroll_Seq", "Target_Table"};
            TypeInformation<?>[] outputTag2_types = {Types.INT, Types.STRING};

            SingleOutputStreamOperator<Row> mainDataStream = ds_processed
                    .process(new ProcessFunction<Row, Row>() {
                        @Override
                        public void processElement(Row row, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out) {
                            // emit data to regular output
//                        out.collect(row);
                            // emit data to side output
                            String target_table = String.valueOf(row.getField(1));
                            switch (target_table){
                                case "TBL_LEARNING_SUBJECT_PROGRESS_RATIO_JAVA":
                                    ctx.output(outputTag1, row);
                                case "TBL_ENROLL_MART_TEST":
                                    ctx.output(outputTag2, row);
                            }
                        }

                    });

            DataStream<Row> ds_sideout1 = mainDataStream.getSideOutput(outputTag1);
            DataStream<Row> ds_sideout2 = mainDataStream.getSideOutput(outputTag2);

            ds_sideout1 = ds_sideout1.map((MapFunction<Row, Row>) row -> row, Types.ROW_NAMED(outputTag1_colums, outputTag1_types));
            ds_sideout2 = ds_sideout2.map((MapFunction<Row, Row>) row -> row, Types.ROW_NAMED(outputTag2_colums, outputTag2_types));

            SideStream.getInstance().executeETL1(ds_sideout1, t_env);
            SideStream.getInstance().executeETL2(ds_sideout2, t_env);

            // 프로그램 실행을 트리거합니다. 환경은 "싱크" 작업을 초래한 프로그램의 모든 부분을 실행합니다.
            // 매개변수로 원하는 작업 이름을 넣고, 프로그램 실행이 기록되고 제공된 이름으로 표시됩니다.
            env.execute("LABS_DATAMART");
        }

    }
}
