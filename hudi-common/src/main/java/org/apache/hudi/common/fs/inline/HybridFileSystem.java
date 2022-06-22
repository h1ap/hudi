/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.fs.inline;

import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * HybridFileSystem is a (key, value) in-memory storage engine and HDFS hybrid file system, whose
 * behavior is expressed in accessing data through the in-memory storage engine if it is a metadata
 * directory file (.hoodie/*), and through the default FileSystem of HDFS when accessing data files.
 * ultimately improving retrieval hoodie's metadata faster.
 */
public class HybridFileSystem extends DistributedFileSystem {

}
