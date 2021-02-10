# Incremental-data-loads-in-spark
Incremental loads/imports means we already have imported the data earlier and not we want to import the delta generated after the last cut. Incremental import based on “last modified” mode for new data inserted.
we want to import all the data which have changed in the table after the timestamp/Date/VersionNumber of our last import. This will include all new data inserted and old data updated.

Let's see how are going to do, first we need to defect the changes and pick it from there.

### High Water Mark Change Detection
This change detection relies on a "high water mark" column capturing the version or time when a row was last updated.The high water mark column must meet the following requirements
- Requirements
    - High water mark column data type should be one any one of the following Date,TimeStamp and Integer
    - All inserts specify a value for the column.
    - All updates to an item also change the value of the column.
    - The value of this column increases with each insert or update. 
    
Queries with the following WHERE clauses can be executed efficiently

### Configs
```scala
jdbc {
  srcTbl {
  options {
  url = "jdbc:oracle:thin:@10.11.12.13:1521:xxx"
  user = "xxxxx"
  password = "xxxxx"
  driver = "oracle.jdbc.driver.OracleDriver"
  dbtable = "(select * from tableA where loadDt > to_timestamp('%s' , 'YYYY-MM-DD HH24:MI:SS.FF')) dta"
  isolationLevel = "NONE"
}
highWaterMarkColumnName = "tableA.loadDt.Timestamp"
highWaterMarkDir = "/data/check-pointing"
}
}
```    
### Data loads
- First time, no water marks, so it will start from beginning to upto now and write into target system, Record the High water value
  - Dataset
    ```
    +----------+-----------------------+
    |TXN_NUMBER|LOADDT                 |
    +----------+-----------------------+
    |100101    |2021-02-10 07:14:51.076|
    |100102    |2021-02-10 07:15:16.893|
    |100103    |2021-02-10 07:15:22.232|
    |100104    |2021-02-10 07:15:27.276|
    |100105    |2021-02-10 07:15:31.809|
    +----------+-----------------------+
    ```
- Second time, Get the high water mark and fetch the latest records based on high watermark value
  - High water mark dataset
    ```
    +------------------------------+-----------------+
    |hwm_key                       |hwm_value        |
    +------------------------------+-----------------+
    |db.tablea.loaddt.timestamp    |20210210071531809|
    +------------------------------+-----------------+
    ```
  - Dataset
  ```
  +----------+-----------------------+
  |TXN_NUMBER|LOADDT                 |
  +----------+-----------------------+
  |100106    |2021-02-10 07:28:22.511|
  |100107    |2021-02-10 07:28:27.778|
  |100108    |2021-02-10 09:43:43.508|
  +----------+-----------------------+
  ```
- Check new water mark have been added 
  
  ```
  +------------------------------+-----------------+
  |hwm_key                       |hwm_value        |
  +------------------------------+-----------------+
  |biuser.tablea.loaddt.timestamp|20210210071531809|
  |biuser.tablea.loaddt.timestamp|20210210094343508|
  +------------------------------+-----------------+
  ```