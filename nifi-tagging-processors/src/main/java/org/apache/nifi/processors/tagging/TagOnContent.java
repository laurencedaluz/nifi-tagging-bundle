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
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

//TODO: add logging

/** Configs
 TagOnContent
    - Attribute to Update
    - Match Requirement
        | exact
        | contain match
    - Support multiple matches
    - Character Set
    - Content Buffer Size
    - [Dynamic Properties]

 TagOnContentWithMapping
    - Attribute to Update
    - Match Requirement
        | exact
        | contain match
    - Support multiple matches
    - Character Set
    - Content Buffer Size
    - Mapping File
    - Mapping File Refresh Interval


 */



@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"identify", "tag", "attribute", "content", "regex", "regular expression", "regexp"})
@CapabilityDescription("Applies Regular Expressions to the content of a FlowFile and tags the FlowFile with a user defined attribute "
        + "based on which Regular Expression matches. Regular Expressions are added as User-Defined Properties where the name "
        + "of the property is the tag and the value of the property is a Regular Expression to match against the FlowFile "
        + "content. User-Defined properties do support the Attribute Expression Language, but the results are interpreted as "
        + "literal values, not Regular Expressions")
@DynamicProperty(name = "Tag Name", value = "A Regular Expression", supportsExpressionLanguage = true, description = "Tags FlowFiles whose "
        + "content matches the regular expression defined by Dynamic Property's value using a tag defined by the Dynamic Property's key")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="User Defined (based on 'Attribute to update' property",
        description="//todo")})
public class TagOnContent extends AbstractProcessor {

    public static final String MATCH_ALL = "content must match exactly";
    public static final String MATCH_SUBSEQUENCE = "content must contain match";

    public static final String SINGLE_TAG = "stop on first match";
    public static final String MULTI_TAG = "tag with all matches";

    public static final PropertyDescriptor ATTRIBUTE_TO_UPDATE = new PropertyDescriptor.Builder()
            .name("Attribute to update")
            .description("Specifies the name of the attribute to be updated with the tag value. ")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .defaultValue("content.tag")
            .build();
    public static final PropertyDescriptor TAG_STRATEGY = new PropertyDescriptor.Builder()
            .name("Tag Strategy")
            .description("Determines if the processor should stop after first match, or if multiple tags are supported. " +
                    "Note - Regular Expressions are evaluated in the order the properties are added to the processor.")
            .required(true)
            .allowableValues(SINGLE_TAG, MULTI_TAG)
            .defaultValue(SINGLE_TAG)
            .build();
    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Content Buffer Size")
            .description("Specifies the maximum amount of data to buffer in order to apply the regular expressions. If the size of the FlowFile "
                    + "exceeds this value, any amount of this value will be ignored")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();
    public static final PropertyDescriptor MATCH_REQUIREMENT = new PropertyDescriptor.Builder()
            .name("Match Requirement")
            .description("Specifies whether the entire content of the file must match the regular expression exactly, or if any part of the file "
                    + "(up to Content Buffer Size) can contain the regular expression in order to be considered a match")
            .required(true)
            .allowableValues(MATCH_ALL, MATCH_SUBSEQUENCE)
            .defaultValue(MATCH_ALL)
            .build();
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles that do not match any of the user-supplied regular expressions will be routed to this relationship")
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles that match any of the user-supplied regular expressions will be routed to this relationship")
            .build();

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_MATCH);
        this.relationships.set(Collections.unmodifiableSet(relationships));

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTE_TO_UPDATE);
        properties.add(TAG_STRATEGY);
        properties.add(MATCH_REQUIREMENT);
        properties.add(CHARACTER_SET);
        properties.add(BUFFER_SIZE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.equals(REL_NO_MATCH.getName())) {
            return null;
        }

        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(1);
        if (flowFiles.isEmpty()) {
            return;
        }

        final AttributeValueDecorator quoteDecorator = new AttributeValueDecorator() {
            @Override
            public String decorate(final String attributeValue) {
                return (attributeValue == null) ? null : Pattern.quote(attributeValue);
            }
        };

        // Set flowfile attribute map
        final Map<FlowFile, Map<String, String>> flowFileAttributeMap = new HashMap<>();
        final ComponentLog logger = getLogger();
        final String attributeToUpdate = context.getProperty(ATTRIBUTE_TO_UPDATE).getValue();

        // Check tagging strategy
        final boolean singleTag;
        if (context.getProperty(TAG_STRATEGY).getValue().equalsIgnoreCase(SINGLE_TAG)) {
            singleTag = true;
        } else {
            singleTag = false;
        }

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final byte[] buffer = new byte[context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue()];
        for (final FlowFile flowFile : flowFiles) {
            final Map<String, String> attributes = new HashMap<>();
            flowFileAttributeMap.put(flowFile, attributes);

            // Read FlowFile content up to user defined buffer size
            final AtomicInteger bufferedByteCount = new AtomicInteger(0);
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
                }
            });

            final String contentString = new String(buffer, 0, bufferedByteCount.get(), charset);
            int multiAttributeIdentifier = 1;

            // For each dynamic property that is set, evaluate user defined regex
            for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (!descriptor.isDynamic()) {
                    continue;
                }

                final String regex = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile, quoteDecorator).getValue();
                final Pattern pattern = Pattern.compile(regex);
                final boolean matches;
                if (context.getProperty(MATCH_REQUIREMENT).getValue().equalsIgnoreCase(MATCH_ALL)) {
                    matches = pattern.matcher(contentString).matches();
                } else {
                    matches = pattern.matcher(contentString).find();
                }

                if (matches) {
                    if (singleTag) {
                        attributes.put(attributeToUpdate, descriptor.getName());
                        break;
                    } else {
                        final String attributeKey = new StringBuilder(attributeToUpdate).append(".").append(multiAttributeIdentifier).toString();
                        multiAttributeIdentifier++;
                        attributes.put(attributeKey, descriptor.getName());
                    }
                }
            }
        }

        for (final Map.Entry<FlowFile, Map<String, String>> entry : flowFileAttributeMap.entrySet()) {
            FlowFile flowFile = entry.getKey();
            final Map<String, String> attributes = entry.getValue();

            if (attributes.isEmpty()) {
                session.transfer(flowFile, REL_NO_MATCH);
                session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
            } else {

                // Add attributes and route to matched
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_MATCH);
            }
        }
    }
}
