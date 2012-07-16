/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.watcher;

import java.io.File;

/**
 *
 */
public interface FileChangesListener {
    /**
     * Called for every file found in the watched directory during initialization
     * @param file
     */
    void onFileInit(File file);

    /**
     * Called for every subdirectory found in the watched directory during initialization
     * @param file
     */
    void onDirectoryInit(File file);

    /**
     * Called for every new file found in the watched directory
     * @param file
     */
    void onFileCreated(File file);

    /**
     * Called for every file that disappeared in the watched directory
     * @param file
     */
    void onFileDeleted(File file);

    /**
     * Called for every file that was changed in the watched directory
     * @param file
     */
    void onFileChanged(File file);

    /**
     * Called for every new subdirectory found in the watched directory
     * @param file
     */
    void onDirectoryCreated(File file);

    /**
     * Called for every file that disappeared in the watched directory
     * @param file
     */
    void onDirectoryDeleted(File file);
}
