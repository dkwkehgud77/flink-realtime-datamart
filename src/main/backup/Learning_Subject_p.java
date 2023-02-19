import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

public class Learning_Subject_p {

    private static final Logger logger = LoggerFactory.getLogger(Learning_Subject_p.class);
    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config");

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
                    System.err.println(" !! JDBC Driver load 오류: " + e.getMessage());
                    e.printStackTrace();
                }
                // MySQLConnectionPool 객체 생성
                dbPool = new MySQLConnectionPool(
                        RESOURCE_BUNDLE.getString("aurora.url")+"/DataMart?useSSL=false",
                        RESOURCE_BUNDLE.getString("aurora.username"),
                        RESOURCE_BUNDLE.getString("aurora.password"),50);
            }
            try {
                // MySQLConnectionPool 에서 커넥션 pool을 가져옵니다.
                conn = dbPool.getConnection();

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            return conn;
        }

        @Override
        public void flatMap(String s, Collector<Row> rows) throws Exception {
            // FlatMapFunction 인터페이스를 상속받아서, Kafka 로 부터 수집한 메시지를 ETL 작업을 수행한 결과를 Row 데이터로 반환하는 함수를 구현합니다.

            Gson gson = new Gson();
            // Kafka CDC 로그 메시지를 Json 포맷으로 변환시킵니다.
            Map<String,Object> data = gson.fromJson(s, Map.class);
            Map<String,Object> payload = (Map<String, Object>) data.get("payload");
            logger.info(payload.toString());
            String table = payload.get("__table").toString();
            String Learning_Seq = null;

            // TBL_ENROLL, TBL_LEARNING_SUBJECT 토픽의 CDC 로그 데이터 분기 처리 --> Learning_Seq 값 구합니다.
            // Learning_Seq는 데이터 마트 TBL_LEARNING_SUBJECT_PROGRESS_RATIO 테이블의 pk값 입니다.
            if("TBL_ENROLL".equals(table)){
                // 카프카 CDC 로그 데이터(Json)에서 Enroll_Seq, Confirm_Status_Cd 데이터를 추출합니다.
                String Enroll_Seq = String.valueOf((int)Double.parseDouble(payload.get("Enroll_Seq").toString()));
                String Confirm_Status_Cd = payload.get("Confirm_Status_Cd").toString();
                Connection conn = getConnMySQL();
                try (Statement stmt = conn.createStatement()) {
                    String sql = "select Learning_Seq from Hlms.TBL_LEARNING where Enroll_Seq = " + Enroll_Seq;
                    ResultSet rs = stmt.executeQuery(sql);

                    while (rs.next()) {
                        // Enroll_Seq 을 기반으로 TBL_ENROLL 테이블에서  Learning_Seq 데이터를 구합니다.
                        Learning_Seq = rs.getString(1);
                        // Confirm_Status_Cd 값이 LMER01 가 아닌경우 TBL_LEARNING_SUBJECT_PROGRESS_RATIO 에서 Delete_Yn을 'Y'로 업데이트 합니다.
                        if (!"LMER01".equals(Confirm_Status_Cd)) {
                            Row row = Row.ofKind(RowKind.INSERT, Integer.parseInt(Learning_Seq), null, null, null, null, null, null, null, null, "Y");
                            logger.info(String.valueOf(row));
                            rows.collect(row);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("exception msg", e);
                } finally {
                    if (conn != null) {
                        dbPool.returnConnection(conn);
                    }
                }

            } else if ("TBL_LEARNING_SUBJECT".equals(table)) {
                // 카프카 CDC 로그 데이터(Json)에서 Learning_Seq 데이터를 추출합니다.
                Learning_Seq = String.valueOf((int)Double.parseDouble(payload.get("Learning_Seq").toString()));
            }

            // Learning_Seq 값을 기반으로 PROC_LEARNING_SUBJECT_PROGRESS_RATIO 프로시저 호출하여 ETL 작업 --> Row 스트림 데이터(임의의 N개) 반환
            if(Learning_Seq != null && Learning_Seq.length() > 0){
                Connection conn = getConnMySQL();
                try (Statement stmt_proc = conn.createStatement()) {

                    // 단과과정 개인별 수료현황 프로시저(PROC_LEARNING_SUBJECT_PROGRESS_RATIO)를 호출하여 ETL 작업을 수행합니다.
                    String sql_proc = "call PROC_LEARNING_SUBJECT_PROGRESS_RATIO('" + Learning_Seq + "')";
                    ResultSet rs_proc = stmt_proc.executeQuery(sql_proc);

                    // 데이터마트 TBL_LEARNING_SUBJECT_PROGRESS_RATIO 테이블에 Learning_Seq(PK) 값 기준으로 조회하여
                    // 데이터가 존재하면 Update_Date만 업데이트 하고, 데이터가 존재하지 않으면 Insert_Date,Update_Date를 현재 시간으로 삽입합니다.
                    Statement stmt_mart = conn.createStatement();
                    String sql_mart = "select Learning_Seq, Insert_Date, Delete_Yn from DataMart.TBL_LEARNING_SUBJECT_PROGRESS_RATIO_JAVA where Learning_Seq = " + Learning_Seq;
                    ResultSet rs_mart = stmt_mart.executeQuery(sql_mart);

                    while(rs_proc.next()) {
                        // 프로시저를 호출하여 ETL 작업을 수행한 결과 데이터를 Row 객체에 담습니다.
                        String Course_Run_Nm = rs_proc.getString(2);
                        String Course_Id = rs_proc.getString(3);
                        int Course_Run_Seq = rs_proc.getInt(4);
                        int Learner_Seq = rs_proc.getInt(5);
                        float Progress_Ratio = rs_proc.getFloat(6);
                        int Learning_Subject_Seq = rs_proc.getInt(7);

                        Timestamp Insert_Date;
                        Timestamp Update_Date;

                        Date date = new Date();
                        java.sql.Date sqlDate = new java.sql.Date(date.getTime());
                        Timestamp nowDate = new Timestamp(date.getTime());

                        // 데이터가 존재하면 Update_Date만 업데이트 하고, 데이터가 존재하지 않으면 Insert_Date,Update_Date를 현재 시간으로 삽입합니다.
                        if(rs_mart.next()){
                            String Delete_Yn = rs_mart.getString(3);
                            if("Y".equals(Delete_Yn)){
                                Insert_Date = nowDate;
                            }else{
                                Insert_Date = rs_mart.getTimestamp(2);
                            }
                        } else {
                            Insert_Date = nowDate;
                        }
                        Update_Date =  nowDate;
                        String Delete_Yn = rs_proc.getString(8);

                        // 행의 주요 목적은 플링크의 테이블과 SQL 에코시스템과 다른 API를 연결하는 것이다.
                        // 따라서 행은 스키마 부분(필드 포함)으로 구성될 뿐만 아니라 변경 로그의 변경을 인코딩하기 위한 RowKind도 첨부합니다.
                        Row row = Row.ofKind(RowKind.INSERT, Integer.parseInt(Learning_Seq), Course_Run_Nm, Course_Id, Course_Run_Seq, Learner_Seq, Progress_Ratio, Learning_Subject_Seq, Insert_Date, Update_Date, Delete_Yn);
                        logger.info(table + " : " + String.valueOf(row));

                        // Kafka 메시지의 FlatMapFunction 변환이 적용된 임의의 n개의 Row를 반환합니다.
                        rows.collect(row);
                    }
                } catch (Exception e) {
                    logger.error("exception msg", e);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {


        logger.info("Flink Job <LEARNING_SUBJECT_PROGRESS_RATIO> Start!");
        // StreamExecutionEnvironment 는 스트리밍 프로그램이 실행되는 컨텍스트이고, 작업 실행을 제어합니다. (Data Access, Fault Tolerance, Check Point )
        // StreamTableEnvironment는 DataStream API와 통합되는 Table 및 SQL API 프로그램을 만들기 위한 시작점이자 중앙 컨텍스트입니다.
        // DataStream --> Table 변환, Table --> DataStream 변환, 외부 시스템 연결 및 SQL 실행, 경계 없는 데이터 처리를 지원합니다.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment t_env = StreamTableEnvironment.create(env);

        // Kafka CDC 수집 시작시간을 파라메터로 받아서 unixtime으로 변환후 설정한다.
        String cdc_start_time = null;
        long startingOffset = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        if(args != null && args.length >0){
            try{
                cdc_start_time = args[0];
                long unixTime = sdf.parse(cdc_start_time).getTime();
                startingOffset = unixTime / 1000;
            } catch (Exception e) {
                logger.error("exception msg", e);
            }
        }else {
            Date now_time = new Date();
            cdc_start_time = sdf.format(now_time).toString();
            startingOffset = now_time.getTime() / 1000;
        }

        logger.info("Kafka 수집 시간 : " + sdf.parse(cdc_start_time));
        logger.info("Kafka startingOffset : " + startingOffset );

        // Kafka Source를 구성. 카프카 BootstrapServer,Topics,Groupid 값 세팅, 메시지값만 역직렬화, 특정 시점(5분전)을 시작 Offset으로 설정합니다.
        List topics = Arrays.asList(RESOURCE_BUNDLE.getString("kafka.topics").split(",")) ;
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(RESOURCE_BUNDLE.getString("kafka.bootstrap.servers"))
                .setTopics(topics)
                .setGroupId(RESOURCE_BUNDLE.getString("kafka.group"))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())      // 마지막 메시지 Offset 기준으로 수집
//                .setStartingOffsets(OffsetsInitializer.timestamp(startingOffset)) // 특정 시간대 Offset 기준으로 수집
                .build();

        // StreamExecutionEnvironment에서 Kafka Source으로 부터 데이터를 실시간으로 데이터를 수집하는 Data Stream을 생성합니다.
        DataStream<String> ds_kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        String[] colums = {"Learning_Seq", "Course_Run_Nm", "Course_Id", "Course_Run_Seq", "Learner_Seq",
                "Progress_Ratio", "Learning_Subject_Seq", "Insert_Date", "Update_Date", "Delete_Yn"};
        TypeInformation<?>[] types = {Types.INT, Types.STRING, Types.STRING, Types.INT, Types.INT, Types.FLOAT, Types.INT, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.STRING};

        // Kafka DataStream에서 flatMap 변환을 통해 ETL 기능을 적용한 새로운 Data Stream을 생성합니다.
        // 데이터 스트림의 각 요소에 대해 flatMap 변환이 적용되고, FlatMapFunction 호출은 임의의 많은 결과 요소(없음 포함)을 반환할 수 있습니다.
        // input 1개 message --> FlatMapFunction --> output n개 messages , FlatMapFunction = new KafkaToRows()
        DataStream<Row> ds_process = ds_kafka.flatMap(new KafkaToRows(), Types.ROW_NAMED(colums,types));

        // ETL 처리한 Data Stream을 Table로 해석합니다.
        Table src_table =  t_env.fromChangelogStream(ds_process);

        String sink_sql = "CREATE TABLE LEARNING_SUBJECT_PROGRESS_RATIO (\n" +
                "        Learning_Seq INT,\n" +
                "        Course_Run_Nm STRING,\n" +
                "        Course_Id STRING,\n" +
                "        Course_Run_Seq INT,\n" +
                "        Learner_Seq INT, \n" +
                "        Progress_Ratio DECIMAL, \n" +
                "        Learning_Subject_Seq INT, \n" +
                "        Insert_Date TIMESTAMP, \n" +
                "        Update_Date TIMESTAMP, \n" +
                "        Delete_Yn STRING,\n" +
                "        primary key (Learning_Seq) NOT ENFORCED\n" +
                "      ) WITH (\n" +
                "        'connector' = 'jdbc',\n" +
                "        'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "        'url' = '%s/DataMart?useSSL=false',\n" +
                "        'username' = '%s',\n" +
                "        'password' = '%s',\n" +
                "        'table-name' = 'TBL_LEARNING_SUBJECT_PROGRESS_RATIO_JAVA'\n" +
                "      )";
        sink_sql = String.format(sink_sql, RESOURCE_BUNDLE.getString("aurora.url"),RESOURCE_BUNDLE.getString("aurora.username"),RESOURCE_BUNDLE.getString("aurora.password"));

        logger.info(sink_sql);

        // 지정된 SQL 문을 실행하고 실행 결과를 반환합니다
        // SQL문은 DDL/DML/DQL/SHOW/DESSRIBE/EXPLAIN/USE일 수 있습니다.
        // DML 및 DDL 문의 경우 이 메서드는 작업이 제출되면 TableResult를 반환합니다.
        // 여러 파이프라인이 단일 실행의 일부로 하나 이상의 싱크 테이블에 데이터를 삽입해야 하는 경우 사용합니다.
        t_env.executeSql(sink_sql);
        src_table.executeInsert("LEARNING_SUBJECT_PROGRESS_RATIO");

        // 프로그램 실행을 트리거합니다. 환경은 "싱크" 작업을 초래한 프로그램의 모든 부분을 실행합니다.
        // 매개변수로 원하는 작업 이름을 넣고, 프로그램 실행이 기록되고 제공된 이름으로 표시됩니다.
        env.execute("LEARNING_SUBJECT_PROGRESS_RATIO");

    }
}
