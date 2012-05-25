HBasePS
=======
<https://github.com/sentric/HBasePS/>

Description:
------------
Real-time notification on a put to HBase implemented with an HBase coprocessors and prospective search.

For more information please refer to the github project wiki:

Build Notes:
------------
Current pom configured to build against HBase 0.92.0 (cdh4b1). 

Note: unit-tests take some time to execute (up to several minutes), to skip
their execution use -Dmaven.skip.tests=true.


CP Loading With HBase Shell Commands:
-------------------------------------

Required tables

    Table     | Column Families | Qualifiers
    ----------------------------------------
    article      ctn
    account      agent              <agentId>
    report       doc                id

Steps to add the coprocessor:

* Copy coprocessor JARs to HDFS
    1. `hadoop fs -rm /hbaseps-1.0-SNAPSHOT.jar`
    2. `hadoop fs -copyFromLocal target/hbaseps-1.0-SNAPSHOT.jar /`
    3. `hadoop fs -ls /`

* Register coprocessor to table
    4. `disable 'article'`
    5. `alter 'article', METHOD => 'table_att', 'COPROCESSOR'=>'hdfs:///hbaseps-1.0-SNAPSHOT.jar|`    
       `ch.sentric.hbase.coprocessor.ProspectiveSearchRegionObserver|1073741823|solr.home=<path to solr.home>'`
    6. `describe 'article'`
    7. `enable 'article'`

* Steps to remove the coprocessor:
    1. `disable 'article'`
    2. `alter 'article', METHOD => 'table_att_unset', NAME => 'COPROCESSOR$1'`
    3. `describe 'article'`
    4. `enable 'article'`

HBase Version Compatibility:
----------------------------
Compatible with HBase 0.92.xxx

Released under Apache License 2.0.

Author:
-------
Christian Guegi
