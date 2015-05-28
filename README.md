Piggy Converter
====

DSL for filtering and converting log data implemented as a pig UDF.  
(Tested on CDH 5.1.2, java 1.7.0_55)

## build

```
git clone https://github.com/rhase/piggy-converter.git
cd piggy-converter
./gradlew
./gradlew jar
```

## setup test data

```
sudo -u hdfs hdfs dfs -mkdir /user/$USER
sudo -u hdfs hdfs dfs -chown $USER /user/$USER
sudo -u hdfs hdfs dfs -chmod 711 /user

sudo -u hdfs hdfs dfs -mkdir /test
sudo -u hdfs hdfs dfs -chown $USER /test
hdfs dfs -put src/test/resources/test1/access.log /test/
hdfs dfs -put src/test/resources/test1/test.pc /test/
hdfs dfs -put src/test/resources/test1/lookup.list /user/${USER}/
```

## run

```
pig -param script=/test/test.pc -param input=/test/access.log -param output=/test/output src/test/resources/test1/test.pig
hdfs dfs -cat /test/output/part-m*
```

## DSL syntax

```
RULE :
  (CONDITION_CLAUSE)? ACTION_CLAUSE

CONDITION_CLAUSE :
  if [not] CONDITION [and|or CONDITION ...]

ACTION_CLAUSE :
  do ACTION [and ACTION ...]

*** CONDITIONs

* match conditions

column (column name) match (exactly|partialy|regex) (match pattern)

* validation conditions

column (column name) is valid url

*** ACTIONs

* row actions

ignore record

*"ignore" actually just set the tuple to null. You must filter null records later.*

* extract actions

get regex value (escaped regular expression) from column (column name) put (group number) to (column name) [xxx to xxx ...]
get url parameter value from column (column name) put (parameter name) to (column name) [(parameter name) to (column name) ...]

* column convert actions

decode url encoded column (column name)
set (column name) to (value)

* look up actions

lookup (exactly|partialy) column (column name) from list (list file hdfs path) put value to column (column name)
*In case no record was looked up, target column is not updated.*
```

## License
Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0