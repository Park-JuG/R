
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




'
이제부터 공변량을 구하도록 하자.
가장 먼저 코호트를 생성하기 위한 SQL을 만들어 실행했다.
'

sql <- "
          SELECT 1 AS cohort_definition_id,
                measurement.person_id AS subject_id,
                measurement.cohort_start_date,
                measurement.cohort_end_date
          INTO cdmpv531_kdh.cohort_of_interest
          FROM (SELECT  
                        person_id,
                        MIN(measurement_date) AS cohort_start_date,
                        MAX(measurement_date) AS cohort_end_date
                  FROM cdmpv531_kdh.MEASUREMENT 
                  WHERE measurement_concept_id = 3028437
                  GROUP BY person_id, measurement_date) measurement
          INNER JOIN cdmpv532_kdh.OBSERVATION_PERIOD
          ON measurement.person_id = OBSERVATION_PERIOD.person_id
            AND cohort_start_date >= observation_period_start_date
            AND cohort_end_date <= observation_period_end_date
          WHERE DATE_PART('day', cohort_start_date::timestamp - observation_period_start_date::timestamp) >= 365;
          "


executeSql(conn, sql)



'
공변량 집합을 설정하고자 하였다. 기본공변량은 실제 나이 기준으로 구할 수 없고 AgeGroup만 구할 수 있다.
그러므로 AgeGroup 말고 실제 나이로도 구하기 위해 기본 공변량 집합을 사용하지 않고 사전 지정 공변량 집합을 사용했다.
'
library(FeatureExtraction)

covariatesettings1 <- createCovariateSettings(useDemographicsGender = TRUE,
                                              useDemographicsAge = TRUE,
                                              useDemographicsAgeGroup = TRUE,
                                              useDemographicsRace = TRUE,
                                              useDemographicsEthnicity = TRUE,
                                              useDemographicsIndexYear = TRUE,
                                              useDemographicsIndexMonth = TRUE,
                                              useDemographicsIndexYearMonth = TRUE,
                                              useDemographicsPriorObservationTime =  TRUE , 
                                              useDemographicsPostObservationTime =  TRUE ,
                                              useDemographicsTimeInCohort =  TRUE , 
                                              useConditionGroupEraLongTerm = TRUE,
                                              useConditionGroupEraShortTerm = TRUE,
                                              useDrugGroupEraLongTerm = TRUE,
                                              useDrugGroupEraShortTerm = TRUE,
                                              useDrugGroupEraOverlapping = TRUE,
                                              useProcedureOccurrenceLongTerm = TRUE,
                                              useProcedureOccurrenceShortTerm = TRUE,
                                              useDeviceExposureLongTerm = TRUE,
                                              useDeviceExposureShortTerm = TRUE,
                                              useMeasurementLongTerm = TRUE,
                                              useMeasurementShortTerm = TRUE,
                                              useMeasurementRangeGroupLongTerm = TRUE,
                                              useObservationLongTerm = TRUE,
                                              useDcsi = TRUE,
                                              useChads2 = TRUE,
                                              useChads2Vasc = TRUE,
                                              longTermStartDays = -365,
                                              mediumTermStartDays = -180,
                                              shortTermStartDays = -30,
                                              endDays = 0,
                                              includedCovariateConceptIds = c(),
                                              addDescendantsToInclude = FALSE,
                                              excludedCovariateConceptIds = c(),
                                              addDescendantsToExclude = FALSE,
                                              includedCovariateIds = c())
covariatesettings1




'
공변량 집합을 구성했으니, 생성한 공변량 집합으로 1인당 공변량을 생성한다.
'

covariatesData <- getDbCovariateData(connectionDetails = connectionDetails,
                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                      cohortDatabaseSchema = resultsDatabaseSchema,
                                      cohortTable = "cohort_of_interest",
                                      cohortId = 1,
                                      rowIdField = "subject_id",
                                      covariateSettings = covariatesettings1)




'
생성된 1인당 공변량에서 covariates와 covariateId에 대한 설명이 들어 있는 covariateRef로 나누어 변수로 저장해 데이터를 불러오기 쉽게 했다.
'


covariateRef<-as.data.frame(covariatesData$covariateRef)
covariateRef

covariate <- as.data.frame(covariatesData$covariates)
summary(covariate2)
head(covariate2)


#AgeGroup별 공변량 구하기
'
covariateId 중에서 AgeGroup에 해당하는 covariateId를 조건 변수에 저장하고 filter함수를 이용해 조건에 맞는 데이터를 추출하도록 했다.
보기 편하게 하기 위해 rowId와 covariateId 순으로 정렬했다.
'

library(dplyr)
age_condition = (covariate$covariateId == 3) | (covariate$covariateId == 1003) | (covariate$covariateId == 2003) | (covariate$covariateId == 3003) | (covariate$covariateId == 4003) | (covariate$covariateId == 5003) |
                (covariate$covariateId == 6003) | (covariate$covariateId == 7003) | (covariate$covariateId == 8003) | (covariate$covariateId == 9003) | (covariate$covariateId == 10003) |
                (covariate$covariateId == 11003) | (covariate$covariateId == 12003) | (covariate$covariateId == 13003) | (covariate$covariateId == 14003) | (covariate$covariateId == 15003) |
                (covariate$covariateId == 16003) | (covariate$covariateId == 17003) | (covariate$covariateId == 18003) | (covariate$covariateId == 19003) | (covariate$covariateId == 20003) 



age_data <- covariate %>% filter(age_condition) %>% arrange(rowId, covariateId)
age_data <- inner_join(age_data, covariateRef, by="covariateId")
head(age_data)


#Gender별 공변량 구하기
'
covariateId 중에서 성별에 따른 공변량을 구하기 위한 조건을 만들었다.
covariateId == 8532001은 여성을 의미하며, covariateId == 8507001은 남성을 의미한다.
'

gender_condition = (covariate$covariateId == 8532001) | (covariate$covariateId == 8507001)

covgenderdate <- covariate %>% filter(gender_condition) %>% arrange(rowId, covariateId)
head(covgenderdate)


#Age별(실제 나이별) 공변량 구하기
'
covariateId 중에서 실제 나이에 따른 공변량을 구하기 위해 covariateId == 1002 조건을 사용해 데이터를 추출했다.
'

age_year_condition = (covariate$covariateId == 1002)
age_year_data <- covariate %>% filter(age_year_condition) %>% arrange(rowId)
head(age_year_data)


#LDL 장기, 중기, 단기 공변량 구하기
'
LDL 변수 중에서 LongTerm, MediumTerm, ShortTerm을 제각기 구하기 위한 조건을 사용해 데이터를 추출했다.
ShortTerm은 30일 이내로, covariateId가 3028437704이며, 
LongTerm은 365일 이내로, covariateId가 3028437702이다.
MediumTerm은 covariateid가 존재하지 않아 공변량을 구할 수 없었다.
'


ShortTerm <- covariate %>% filter(covariate$covariateId == 3028437704) %>% arrange(rowId)
head(ShortTerm) # day -30 through 0

LongTerm <- covariate %>% filter(covariate$covariateId == 3028437702) %>% arrange(rowId)
head(LongTerm) # day -365 through 0








