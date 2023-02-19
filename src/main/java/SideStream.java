import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SideStream {

    private static SideStream sideStream = new SideStream();

    private SideStream(){}

    public static SideStream getInstance(){
        return sideStream;
    }

    void executeETL1(DataStream<Row> sideOutputStream, StreamTableEnvironment t_env){
        // 반환된 Learning_Seq 데이터 Row의 Data Stream을 Table로 해석합니다.
        // PROCTIME()을 통해 현재 시간을 구하여, 메시지 proc_time 컬럼도 추가하여 Table에 담고, 임시로 src_tbl이라는 View를 생성합니다.
        Table src_table = t_env.fromDataStream(sideOutputStream, Schema.newBuilder().column("Learning_Seq", DataTypes.INT()).columnByExpression("proc_time", "PROCTIME()").build());
        t_env.createTemporaryView("SRC_TBL_1", src_table);
        // Kafka CDC 메시지에서 전처리하고 변환한 Learning_Seq(PK) 데이터가 담긴 View(src_tbl)를 기준으로 DataMart ETL쿼리를 실행합니다.
        // 기준이 되는 src_tbl View 테이블의 현재 시간대(SRC.proc_time)로 다른 테이블들을 Join 하여 ETL 쿼리를 만듭니다.
        String etl_sql =
                "SELECT\n" +
                        "       L.Learning_Seq as Learning_Seq\n" +
                        "      ,CR.Course_Run_Nm as Course_Run_Nm\n" +
                        "      ,C.Course_Id as Course_Id\n" +
                        "      ,CR.Course_Run_Seq as Course_Run_Seq\n" +
                        "      ,L.Learner_Seq as Learner_Seq\n" +
                        "      ,LS.Progress_Ratio as Progress_Ratio\n" +
                        "      ,LS.Learning_Subject_Seq as Learning_Subject_Seq\n" +
                        "      ,IF(LP.Insert_Date is NULL, NOW(), LP.Insert_Date)  as Insert_Date\n" +
                        "      ,NOW() as Update_Date\n" +
                        "      ,CASE WHEN C.Course_Type_Cd = 'LMCT01' AND E.Confirm_Status_Cd ='LMER01' THEN 'N' ELSE 'Y' END as Delete_Yn\n" +
                        "FROM SRC_TBL_1 as SRC\n" +
                        "    INNER JOIN TBL_LEARNING FOR SYSTEM_TIME AS OF SRC.proc_time as L\n" +
                        "ON L.Learning_Seq = SRC.Learning_Seq\n" +
                        "    INNER JOIN TBL_COURSE_RUN FOR SYSTEM_TIME AS OF SRC.proc_time as CR\n" +
                        "ON L.Course_Run_Seq = CR.Course_Run_Seq\n" +
                        "    INNER JOIN TBL_COURSE FOR SYSTEM_TIME AS OF SRC.proc_time as C\n" +
                        "ON CR.Course_Seq = C.Course_Seq\n" +
                        "    INNER JOIN TBL_ENROLL FOR SYSTEM_TIME AS OF SRC.proc_time as E\n" +
                        "ON CR.Course_Run_Seq = E.Course_Run_Seq AND L.Enroll_Seq = E.Enroll_Seq\n" +
                        "    LEFT JOIN TBL_LEARNING_SUBJECT FOR SYSTEM_TIME AS OF SRC.proc_time as LS\n" +
                        "ON LS.Learning_Seq = L.Learning_Seq\n" +
                        "    LEFT JOIN TBL_LEARNING_SUBJECT_PROGRESS_RATIO FOR SYSTEM_TIME AS OF SRC.proc_time as LP\n" +
                        "ON LP.Learning_Seq = L.Learning_Seq\n";
        Table etl_table = t_env.sqlQuery(etl_sql);
        etl_table.executeInsert("TBL_LEARNING_SUBJECT_PROGRESS_RATIO");
    }

    void executeETL2(DataStream<Row> sideOutputStream, StreamTableEnvironment t_env){
        // 반환된 Learning_Seq 데이터 Row의 Data Stream을 Table로 해석합니다.
        // PROCTIME()을 통해 현재 시간을 구하여, 메시지 proc_time 컬럼도 추가하여 Table에 담고, 임시로 src_tbl이라는 View를 생성합니다.
        Table src_table = t_env.fromDataStream(sideOutputStream, Schema.newBuilder().column("Enroll_Seq", DataTypes.INT()).columnByExpression("proc_time", "PROCTIME()").build());
        t_env.createTemporaryView("SRC_TBL_2", src_table);

        // Kafka CDC 메시지에서 전처리하고 변환한 Learning_Seq(PK) 데이터가 담긴 View(src_tbl)를 기준으로 DataMart ETL쿼리를 실행합니다.
        // 기준이 되는 src_tbl View 테이블의 현재 시간대(SRC.proc_time)로 다른 테이블들을 Join 하여 ETL 쿼리를 만듭니다.
        String etl_sql =
                "SELECT\n" +
                        "      E.Enroll_Seq as Enroll_Seq\n" +
                        "      ,EM.Course_Run_Seq as Course_Run_Seq\n" +
                        "      ,IF(EM.Insert_Date is NULL, NOW(), EM.Insert_Date)  as Insert_Date\n" +
                        "      ,NOW() as Update_Date\n" +
                        "      ,'N' as Delete_Yn\n" +
                        "FROM SRC_TBL_2 as SRC\n" +
                        "    INNER JOIN TBL_ENROLL FOR SYSTEM_TIME AS OF SRC.proc_time as E\n" +
                        "ON E.Enroll_Seq = SRC.Enroll_Seq\n" +
                        "    LEFT JOIN TBL_ENROLL_MART_TEST FOR SYSTEM_TIME AS OF SRC.proc_time as EM\n" +
                        "ON E.Enroll_Seq = EM.Enroll_Seq";

        Table etl_table = t_env.sqlQuery(etl_sql);
        // ETL Query가 실행 및 처리된 데이터는 Flink Table 객체에 담습니다.
        // Sink Table에 ETL 처리된 Table 데이터를 Upsert 합니다.

        etl_table.executeInsert("TBL_ENROLL_MART_TEST");
    }
}
