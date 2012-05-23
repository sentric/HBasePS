/**
 * Copyright 2012 Sentric AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.sentric.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 */
public class LoadWithTableDescriptorExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(fs.getUri() + Path.SEPARATOR + "coprocessor-1.0-SNAPSHOT.jar");

    HTableDescriptor htd = new HTableDescriptor("testtable");
    htd.addFamily(new HColumnDescriptor("colfam1"));
    htd.setValue("COPROCESSOR$1", path.toString() +
      "|" + ProspectiveSearchRegionObserver.class.getCanonicalName() + 
      "|" + Coprocessor.PRIORITY_USER);

    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(htd);

    System.out.println(admin.getTableDescriptor(Bytes.toBytes("testtable")));
  }
}