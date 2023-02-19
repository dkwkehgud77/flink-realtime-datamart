# 1. 개발환경 세팅 및 테스트

```bash
# 1) mac에서 maven 설치
$ brew install maven 

# 1-1) linux에서 maven 설치 및 환경변수 설정 
$ wget http://mirror.apache-kr.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
$ tar -xvf apache-maven-3.6.3-bin.tar.gz
$ ln -s apache-maven-3.6.3-bin maven
$ vi ~/.bash_profile
export MAVEN_HOME=/usr/local/maven
PATH=$PATH:$HOME/bin:$MAVEN_HOME/bin
export PATH

# 2) maven 설치 확인 
$ mvn -version
$ which mvn 

# 3) git 소스 다운로드 
$ git clone http://gitlab.hunet.co.kr/ai/datapipeline/flink-java.git

# 4) maven 라이브러리 다운로드 및 소스 컴파일
# 서버에서 실행하는 것을 추천드립니다. 내부망은 인터넷이 안되기 때문...
$ cd flink-java
$ mvn compile 

# 5) maven 프로젝트 실행 
# pom.xml.DEV --> 내용 복사 --> pom.xml 
# pom.xml에 flink 라이브러리 3개 scope가 provided인 태그들 주석 처리하고 실행해야 합니다.
# Kafka CDC 현재 시간으로 시작
$ mvn exec:java
# Kafka CDC 지정 시간으로 시작 
$ mvn exec:java -Dexec.args="20220928133000"

# 6) pom.xml scope 참고 사항
# flink 라이브러리의 scope가 provided 지정은 jar 파일로 배포 및 flink run으로 실행할때 사용됩니다. 
# 외부 컨테이너에서 기본 제공되는 API인 경우, provided로 지정하면 마지막 패키징할때 포함되지 않습니다. 
# maven으로 jar 파일로 패키징 하면, target/<artifactId>-<version>.jar 생성 됩니다.  
$ mvn clean 
$ mvn pacakge
```


# 2. 애플리케이션 구조
    .
    ├── src/main  애플리케이션 
    │     ├── java         
    │     │     ├── MainStream.java        Flink 컨슈머 코드              
    │     │     └── MySQLConnectionPool.java     MySQL 커넥션 관련 소스 
    │     └── resources    환경 설정 (log4j2, config) 
    └── target   패키징 시 jar파일 생성    

# 3. Flink 컨슈머 구조
    .
    ├── MainStream.java  Flink 컨슈머 코드
    │     ├── KafkaToRows         
    │     │     ├── Connection getConnMySQL()                       MySQL 커넥션풀 객체 연결                 
    │     │     └── void flatMap(String s, Collector<Row> rows)     Kafka DataStream의 각 요소에 대해 flatMap 변환 적용   
    │     │           ├──  Map<String,Object> payload = gson.fromJson(s, Map.class).get("payload")       Kafka CDC 로그 메시지를 Json 포맷으로 변환   
    │     │           ├──  String Learning_Seq = payload.get("Learning_Seq").toString()                  CDC 로그 메시지에서 Learning_Seq 추출 - pk값  
    │     │           ├──  stmt_proc.executeQuery("call PROC_LEARNING_SUBJECT ..("'+Learning_Seq+'")")   개인별 수료현황 프로시저 호출 ETL 작업을 수행
    │     │           ├──  Row row = Row.ofKind(RowKind.INSERT, Learning_Seq,Course_Run_Nm, ...)         ETL 작업을 수행 결과 데이터 Row 객체 생성 
    │     │           └──  rows.collect(row);                                                            FlatMapFunction 변환이 적용된 임의의 n개의 Row를 반환
    │     │
    │     └── void main()   애플리케이션 메인 로직 실행  
    │           ├── KafkaSource<String> source = KafkaSource.<String>builder()..                    Kafka Brokers, Topics 연결 및 Source 객체 구성
    │           ├── DataStream<String> ds_kafka = env.fromSource(source, ...)                       Kafka 메시지를 실시간으로 수집하는 DataStream 생성 
    │           ├── DataStream<Row> ds_process = ds_kafka.flatMap(new KafkaToRows(), ...)           Kafka flatMap 변환을 통해 ETL 작업을 수행한 DataStream 생성
    │           ├── Table src_table =  t_env.fromChangelogStream(ds_process)                        ETL 작업을 수행한 DataStream을 Table로 해석
    │           ├── String sink_sql = "CREATE TABLE LEARNING_SUBJECT_PROGRESS_RATIO (...)           DDL 문의 작업이 제출되면 TableResult를 반환 
    │           └── src_table.executeInsert("LEARNING_SUBJECT_PROGRESS_RATIO");                     DataStream --> Table 해석된 데이터가 Sink 테이블에 삽입
    │
    └── MySQLConnectionPool.java   MySQL 커넥션풀 객체           

# 4. 배포 및 Flink Job 실행
```bash
# 1) maven 패키지 jar파일 생성 (개발) 
# pom.xml에 flink 라이브러리 3개 scope가 provided인 태그들 주석 처리 해제하고, 패키징 합니다. 
src/main/resources/config.properties.DEV --> 내용 복사 --> config.properties 
$ mvn clean 
$ mvn pacakge
$ scp ./target/MainStream-0.1.jar hunetailab@172.31.32.40:~/flink/scripts/java

# 1-1) maven 패키지 jar파일 생성 (운영)   
src/main/resources/config.properties.PROD --> 내용 복사 --> config.properties 
$ mvn clean 
$ mvn pacakge
$ scp ./target/MainStream-0.1.jar hunetailab@10.140.19.40:~/flink/scripts/java

# 2) flink로 jar 애플리케이션 실행 
$ cd ~/flink
$ ./bin/flink run -d ./scripts/java/MainStream-0.1.jar 20220929133000  <-- Kafka CDC 시작 시간 

# 3) flink web-ui를 통해 job 실행 모니터링 
http://10.140.19.40:8081/#/overview  

위의 url에 접속하여 아래의 두개의 job이 실행중인지 확인 
Job > Running Jobs > LEARNING_SUBJECT_PROGRESS_RATIO
Job > Running Jobs > insert-into_default_catalog.default_database.LEARNING_SUBJECT_PROGRESS_RATIO

# 4) flink web-ui를 통해 job 로그 확인 
Job > Running Jobs > LEARNING_SUBJECT_PROGRESS_RATIO > Exceptions 
예외처리나 오류가 있었는지 확인 

Job > Running Jobs > LEARNING_SUBJECT_PROGRESS_RATIO > [Source: Kafka Source -> Flat Map] > TaskManagers > Logs 
Kafka 메시지 --> Row 데이터가 수집 되는지, 오류가 있는지 맨 하위의 Logs 주기적으로 확인 
```
    

