
library(SqlRender)

# Details for connecting to the server:
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "dbms",
                                                                server ="server",
                                                                user = "user",
                                                                password = "user",
                                                                port = port)



#DB 커넥트
conn <- connect(connectionDetails)

dbDisconnect(conn) # DB 커넥트 제거


# LDL 수치, 환자 ID, 측정 날짜 구하기

LDL <- querySql(conn, "SELECT measurement_id, person_id, measurement_date, value_as_number 
                        FROM cdmpv531_kdh.MEASUREMENT 
                        WHERE measurement_concept_id = 3028437 
                        ORDER BY PERSON_ID, measurement_date;")
LDL

write.csv(LDL, "C:\\Users\\user\\Documents\\LDL.csv")


# 심근경색 발생 여부 구하기
'
바로 위에서 구한 환자 ID를 사용하기 위해 SELECT문으로 person_id만을 중복되지 않게 하여 데이터로 뽑아냈다.
'

people <- querySql(conn, "SELECT distinct(person_id)
                        FROM cdmpv531_kdh.MEASUREMENT 
                        WHERE measurement_concept_id = 3028437
                        ORDER BY PERSON_ID;")
people


' 
심근 경색 발생한 환자 데이터
condition_concept_id = 312327은 Acute myocardial infarction, condition_concept_id = 314666은 Old myocardial infarction, condition_concept_id = 4108217은 Subsequent myocardial infarction이다.
위의 condition_concept_id 중 하나라도 해당 되는 환자는 임의로 만든 myocardial_infarction 란에 YES 라고 출력되게 했다.
'
myo <- querySql(conn, "SELECT DISTINCT(person_id), 'YES' AS myocardial_infarction  
                        FROM cdmpv531_kdh.CONDITION_OCCURRENCE 
                        WHERE (condition_concept_id = 312327 or condition_concept_id= 314666 or condition_concept_id=4108217) 
                        AND person_id IN (SELECT person_id FROM cdmpv531_kdh.MEASUREMENT WHERE measurement_concept_id = 3028437);")
myo

'
보다 쉽게 데이터를 결합하기 위해 dplyr 패키지를 사용했다.
'
library(dplyr)

'
myo는 심근 경색이 발생한 환자들만을 추출한 데이터이기 때문에 people 데이터와 myo 데이터를 일반적인 조인을 사용하여 데이터를 합치면 심근경색이 발생하지 않은 환자들의 데이터는 사라진다.
심근경색이 발생하지 않은 환자들의 데이터도 남게 하기 위해 full join을 이용하여 두 데이터를 합쳤다.
합친 이후, 심근경색이 발생하지 않은 환자들은 MYOCARDIAL_INFARCTION란에 결측값이 나오기 때문에 결측값을 NO라고 변경해 주었다.
'

data <- full_join(people, myo, by="PERSON_ID")
data$MYOCARDIAL_INFARCTION[is.na(data$MYOCARDIAL_INFARCTION)] <- "NO"
data

write.csv(c, "C:\\Users\\user\\Documents\\LDL_MYOCARDIAL_INFARCTION_happens.csv")








