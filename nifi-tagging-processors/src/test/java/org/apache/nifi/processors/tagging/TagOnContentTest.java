/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.tagging;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Before;
import org.junit.Test;



public class TagOnContentTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(TagOnContent.class);
    }

    @Test
    public void singleTag() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TagOnContent());
        runner.setProperty(TagOnContent.MATCH_REQUIREMENT, TagOnContent.MATCH_SUBSEQUENCE);
        runner.setProperty(TagOnContent.ATTRIBUTE_TO_UPDATE, "mytag");
        runner.setProperty("hi", "Hello");
        runner.setProperty("there", "Human");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));

        runner.run();
        runner.assertAllFlowFilesContainAttribute(TagOnContent.REL_MATCH, "mytag");
        runner.getFlowFilesForRelationship(TagOnContent.REL_MATCH).get(0).assertAttributeEquals("mytag", "hi");
        runner.assertTransferCount(TagOnContent.REL_MATCH, 1);
        runner.assertTransferCount(TagOnContent.REL_NO_MATCH, 0);
    }

    @Test
    public void multiTag() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TagOnContent());
        runner.setProperty(TagOnContent.MATCH_REQUIREMENT, TagOnContent.MATCH_SUBSEQUENCE);
        runner.setProperty(TagOnContent.ATTRIBUTE_TO_UPDATE, "mytag");
        runner.setProperty(TagOnContent.TAG_STRATEGY, TagOnContent.MULTI_TAG.getValue());
        runner.setProperty("hi", "Hello");
        runner.setProperty("there", "World");



        runner.enqueue(Paths.get("src/test/resources/hello.txt"));

        runner.run();
        runner.assertAllFlowFilesContainAttribute(TagOnContent.REL_MATCH, "mytag.1");
        runner.assertAllFlowFilesContainAttribute(TagOnContent.REL_MATCH, "mytag.2");
        runner.assertTransferCount(TagOnContent.REL_MATCH, 1);
        runner.assertTransferCount(TagOnContent.REL_NO_MATCH, 0);
    }

    @Test
    public void noTag() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TagOnContent());
        runner.setProperty(TagOnContent.MATCH_REQUIREMENT, TagOnContent.MATCH_SUBSEQUENCE);
        runner.setProperty(TagOnContent.ATTRIBUTE_TO_UPDATE, "mytag");
        runner.setProperty("attr", "GoodBye");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));

        runner.run();
        runner.assertTransferCount(TagOnContent.REL_MATCH, 0);
        runner.assertTransferCount(TagOnContent.REL_NO_MATCH, 1);
    }

    @Test
    public void testBufferSize() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TagOnContent());
        runner.setProperty(TagOnContent.MATCH_REQUIREMENT, TagOnContent.MATCH_ALL);
        runner.setProperty(TagOnContent.BUFFER_SIZE, "3 B");
        runner.setProperty("rel", "Hel");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));

        runner.run();
        runner.assertAllFlowFilesTransferred(TagOnContent.REL_MATCH, 1);
    }



}
