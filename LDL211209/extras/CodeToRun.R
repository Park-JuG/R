
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

myo_data <- left_join(people, myo, by="PERSON_ID")
myo_data$MYOCARDIAL_INFARCTION[is.na(myo_data$MYOCARDIAL_INFARCTION)] <- "NO"
myo_data

write.csv(myo_data, "C:\\Users\\user\\Documents\\LDL_MYOCARDIAL_INFARCTION_happens.csv")


#사망자 데이터 구하기
'
LDL이 있던 환자들 중 사망한 환자들과 사망하지 않은 환자들을 구하기 위해 LDL에 해당하는 환자들의 id로 DEATH 테이블에서 환자 ID와 함께 YES라는 데이터를 추출하도록 했다.
이후, people 데이터에 환자 ID를 기준으로 death 데이터를 왼쪽 조인 하고 YES값이 없는 데이터에 NO라는 값이 나오도록 하여
death 속성에서 환자들의 사망 여부를 볼 수 있게 했다.
'
death <- querySql(conn,"SELECT person_id, 'YES' AS death
                        FROM cdmpv531_kdh.DEATH 
                        WHERE person_id IN (SELECT person_id FROM cdmpv531_kdh.MEASUREMENT WHERE measurement_concept_id = 3028437)")

death_data <- left_join(people, death, by="PERSON_ID")
death_data$DEATH[is.na(death_data$DEATH)] <- "NO"
head(death_data)




#공변량 구하기
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



#데이터 셋 만들기
'
LDL 데이터와 공변량 데이터, outcome 데이터를 이용해 필요한 데이터 셋을 만들 것이다.
LDL 데이터는 measurement_date로 인해 중복되는 데이터가 있기에 따로 데이터셋을 만들 것이고,
공변량 데이터와 outcome 데이터는 합쳐서 하나의 데이터 셋으로 만들 것이다.

LDL 데이터를 데이터 셋으로 만들기에 앞서 LDL이 어떤 구조의 자료인지 확인하기로 하자.
'
class(LDL) #data.frame

'
LDL 데이터는 이미 data.frame 형태로 구성되어 있어, 따로 자료 구조를 변경하지 않았다.

이제 공변량 데이터와 outcome 데이터를 합쳐서 하나의 데이터셋으로 만들자.
먼저 공변량 데이터들끼리 조인시키자.
실제 나이값 공변량과 연령 그룹별 공변량, 성별 공변량, 장기 공변량, 단기 공변량을 먼저 조인시켜 공변량 데이터 셋을 만들었다.
이후, 공변량 속성명으로 어느 속성을 의미하는 것인지 몰라 rename 함수로 속성명을 변경했다.
그리고 잘 변경되었는지 확인했다.
'
cov_data <- age_year_data %>% inner_join(age_data, by="rowId") %>% inner_join(gender_data, by="rowId") %>% 
                              left_join(long_term_data, by="rowId") %>% left_join(short_term_data, by="rowId")

cov_data <- rename(cov_data, ageYearCovariateId = covariateId.x, ageYearCovValue = covariateValue.x,
                             ageCovariateId = covariateId.y, ageCovValue = covariateValue.y,
                             genderCovariateId = covariateId.x.x, genderCovValue = covariateValue.x.x,
                             longTermCovariateId = covariateId.y.y, longTermCovValue = covariateValue.y.y,
                             shortTermCovariateId = covariateId, shortTermCovValue = covariateValue)
head(cov_data)

'
이후, 만들어진 공변량 데이터셋에 left 조인으로 앞서 구한 데이터인 심근경색 데이터와 사망자 데이터를 rowId와 PERSON_ID를 기준으로 합쳤다.
잘 합쳐졌는지 확인하고 str() 함수로 어떤 자료구조 형태를 띄는지 확인했다.
out_cov_data 역시 data.frame 형태로 구성되어 있었기 때문에 자료 구조를 변경하지 않았다.
'
out_cov_data <- cov_data %>% left_join(myo_data, by=c("rowId" = "PERSON_ID")) %>% left_join(death_data, by=c("rowId" = "PERSON_ID"))
head(out_cov_data)
str(out_cov_data)


'
이로써 LDL 데이터셋과 공변량_결과 데이터셋이 만들어졌다.
'




# LDL 그룹 나누고 분석하기
'
LDL 그룹을 LDL 수치의 분산이 높은 그룹과 LDL 수치의 분산이 낮은 그룹으로 낮은 그룹으로 나눌 것이다.
그러기에 앞서 분산을 성공적으로 얻기 위해 데이터가 하나인 사람을 제외하고 분산을 구한 뒤,
분산의 중위값을 구할 것이다. 
'

# 데이터가 하나인 사람 제외
LDL2 <- LDL %>% group_by(PERSON_ID) %>% summarise(n=n())
LDL2 <- LDL2 %>% filter(n!=1)
LDL2 <- inner_join(LDL2, LDL, by="PERSON_ID")
LDL2 <- LDL2 %>% select(PERSON_ID, MEASUREMENT_ID, MEASUREMENT_DATE, VALUE_AS_NUMBER)
head(LDL2)

# 사람별 LDL 분산 구하기
LDL_var <- LDL2 %>% group_by(PERSON_ID) %>% summarise(var = var(VALUE_AS_NUMBER))
LDL_median <- LDL_var %>% summarise(median = median(var))
LDL_median <- as.numeric(LDL_median) 
LDL_median 


# 중위값 기준으로 나누기
higher <- LDL_var %>% filter(var >= LDL_median) %>% mutate(group = "Higher")
lower <- LDL_var %>% filter(var < LDL_median) %>% mutate(group = "Lower")
LDL_data <- rbind(higher, lower)
LDL_data <- LDL_data %>% arrange(PERSON_ID)
head(LDL_data)



# 그룹별 비교
'
이제 LDL 수치 분산이 높은 그룹과 낮은 그룹별로 심근경색 발생, 사망자 수, 실제나이, 나이그룹, 성별에 차이가 있는지 확인할 수 있다.
tableone 패키지를 사용할 것이다.
'
library(tableone)

# 심근경색 비교
# 데이터 생성
myoVar_data <- inner_join(LDL_data, myo_data, by="PERSON_ID")
head(myoVar_data)
str(myoVar_data)


# 비교분석
'
myoVars에 내가 비교할 데이터 속성명을 저장하고, catVars에 범주형 데이터 속성명을 저장하고 분석을 실시한다.
비정규 데이터가 있을수 있으므로 먼저 계층별이 아닌 비교분석을 실시한 후, 비정규 데이터는 biomarkers 변수에 집어넣고 재분석한다.
'
dput(names(myoVar_data))
myoVars <- c("var", "group","MYOCARDIAL_INFARCTION")
catVars <- c("group","MYOCARDIAL_INFARCTION")
myoTab <- CreateTableOne(vars=myoVars, data=myoVar_data, factorVars=catVars)
myoTab

myoTab <- CreateTableOne(vars=myoVars, strata = "group", data=myoVar_data, factorVars = catVars)
biomarkers <- c("var")
print(myoTab, nonnormal = biomarkers, formatOptions = list(big.mark = ",")) # 그룹별 심근경색 비교

'결과는 p-value가 0.001보다 작으므로, 그룹별 심근경색 발생률은 차이가 존재한다고 할 수있다.'





# 사망자 수 비교
# 데이터 생성
deathVar_data <- inner_join(LDL_data, death_data, by = "PERSON_ID")
head(deathVar_data)
str(deathVar_data)

# 비교분석
dput(names(deathVar_data))
deathVars <- c("var", "group","DEATH")
catVars <- c("group","DEATH")
deathTab <- CreateTableOne(vars=deathVars, data=deathVar_data, factorVars=catVars)
deathTab

deathTab <- CreateTableOne(vars=deathVars, strata = "DEATH", data=deathVar_data, factorVars = catVars)
biomarkers <- c("var")
print(deathTab, nonnormal = biomarkers, formatOptions = list(big.mark = ",")) # 그룹별 사망자 비교

'결과는 p-value가 0.001보다 작으므로, 그룹별 사망자 수에 차이가 존재할 수 있다고 할 수 있다.'






# 그룹별 공변량 비교
# 데이터 생성
covVar <- inner_join(cov_data, LDL_data, by=c("rowId" = "PERSON_ID"))
head(covVar)
str(covVar)


# 실제나이 비교
ageYearVars <- c("var", "ageYearCovariateId", "ageYearCovValue", "group")
catVars <- c("group", "ageYearCovariateId")
ageYearTab <- CreateTableOne(vars=ageYearVars, data=covVar, factorVars=catVars)
ageYearTab

ageYearTab <- CreateTableOne(var=ageYearVars, strata = "group", data=covVar, factorVars = catVars)
biomarkers <- c("var", "ageYearCovValue")
print(ageYearTab, nonnormal=biomarkers, formatOptions = list(big.mark=",")) # 그룹별 실제나이비교
'
결과는 ageYearCovValue의 p-value가 0.722이므로 귀무가설을 기각하지 못한다.
즉, 그룹별 실제나이의 차이는 크지 않는다고 할 수 있다.
'



# 나이그룹 비교
ageVars <- c("var", "ageCovariateId", "ageCovValue", "group")
catVars <- c("group", "ageCovariateId")
ageTab <- CreateTableOne(vars=ageVars, data=covVar, factorVars=catVars)
ageTab

ageTab <- CreateTableOne(var=ageVars, strata = "group", data=covVar, factorVars = catVars)
biomarkers <- c("var", "ageCovValue")
print(ageTab, nonnormal = biomarkers, formatOptions = list(big.mark=",")) # 그룹별 나이그룹 비교
'
결과는 ageCovariateId의 p-value가 0.421로, 귀무가설을 기각하지 못함으로써,
그룹별 나이그룹도 당연하게도 차이가 크지 않다고 할 수 있다.
'


# 성별 비교
genderVars <- c("var", "genderCovariateId", "genderCovValue", "group")
catVars <- c("group", "genderCovariateId")
genderTab <- CreateTableOne(vars=genderVars, data=covVar, factorVars = catVars)
genderTab


genderTab <- CreateTableOne(var=genderVars, strata = "group", data=covVar, factorVars=catVars)
biomarkers <- c("var","genderCovValue")
print(genderTab, nonnormal=biomarkers, formatOptions=list(big.mark=",")) # 그룹별 성별 비교
'
결과는 genderCovariateId의 p-value가 0.619로, 귀무가설을 기각하지 못한다.
즉, 그룹별 성별에도 큰 차이도 나지 않는다.
'





#kaplan-meier 분석
library(survival)
library(survminer)

# 데이터 생성
LDL3 <- LDL2 %>% group_by(PERSON_ID) %>% mutate(survTime = max(MEASUREMENT_DATE) - min(MEASUREMENT_DATE))
survTime <- LDL3 %>% select(PERSON_ID, survTime) %>% unique()

# 그룹별 사망자 생존곡선
survData <- left_join(deathVar_data, survTime, by = "PERSON_ID")
survData <- survData %>% mutate(DEATH = ifelse(survData$DEATH == "NO", 0, 1))
str(survData)

fit = survfit(Surv(survData$survTime, survData$DEATH)~group, data=survData)
fit
summary(fit)

plot(fit, ylab="Survival", xlab="Days", ylim=c(0.8, 1.0), col=c("red","blue"), lty="solid", lwd = 1, mark.time=T)
legend("bottomleft", legend=levels(group), col=c("red","blue"), lty="solid")


# 그룹별 심근경색 발생 생존곡선
survData2 <- left_join(myoVar_data, survTime, by="PERSON_ID")
survData2 <- survData %>% mutate(MYOCARDIAL_INFARCTION = ifelse(survData2$MYOCARDIAL_INFARCTION == "NO",0,1))
str(survData2)

fit = survfit(Surv(survData2$survTime, survData2$MYOCARDIAL_INFARCTION)~group, data=survData2)
fit
summary(fit)

plot(fit, ylab="Survival", xlab="Days", ylim=c(0.3, 1.0), col=c("red","blue"), lty="solid", lwd = 1, mark.time=T)
legend("bottomleft", legend=levels(group), col=c("red","blue"), lty="solid")


# 콕스 분석
'콕스 분석은 독립변수에 group변수 뿐만 아니라 ageYearCovValue와 genderCovariateId를 추가해 분석할 것이다.'

# 사망자 콕스 분석
survData <- survData %>% select(PERSON_ID, survTime, DEATH)
coxData <- left_join(survData, covVar, by= c("PERSON_ID" = "rowId")) 
head(coxData)
str(coxData)

coxFit <- coxph(Surv(survData$survTime, survData$DEATH)~group+ageYearCovValue+genderCovariateId, data=coxData)
coxFit

'
LDL 값이 작은 그룹은 큰 그룹의 HR의 1.15배이며, 성별이 여성이면 남성의 HR의 0.88배이며, 연령은 한 살 더 먹을 대마다 0.99배가 된다.
따라서 LDL이 작은 그룹이 큰 그룹에 비해 사망할 확률이 더 높으며,
남성은 여성에 비해 사망할 확률이 높고
나이가 먹을 수록 사망할 확률이 낮다고 할 수 있다.
'

# 심근경색 발생 콕스 분석
survData2 <- survData2 %>% select(PERSON_ID, survTime, MYOCARDIAL_INFARCTION)
coxData <- left_join(survData2, covVar, by=c("PERSON_ID" = "rowId"))
head(coxData)
str(coxData)

coxFit <- coxph(Surv(survData2$survTime, survData2$MYOCARDIAL_INFARCTION)~group+ageYearCovValue+genderCovariateId, data=coxData)
coxFit
'
LDL 수치 분산이 작은 그룹은 큰 그룹의 HR에 비해 0.812배이며, 성별이 여성이면 남성의 HR에 비해 0.817배이고, 연령은 한 살을 더 먹을 대마다 0.99배가 된다.
즉, LDL 수치 분산이 작은 그룹은 큰 그룹에 비해 심근경색이 발생할 확률이 더 낮으며,
여성은 남성에 비해 심근경색이 발생할 확률이 낮고,
나이는 먹을 수록 심근경색이 발생할 확률이 낮다고 할 수 있다.
'
