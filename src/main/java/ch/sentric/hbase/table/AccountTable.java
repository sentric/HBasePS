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
package ch.sentric.hbase.table;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 */
public class AccountTable {
    public static final byte[] NAME = Bytes.toBytes("account");
    public static final byte[] AGENT_FAMILIY = Bytes.toBytes("agent");
}
