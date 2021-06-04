/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.common.xcontent.XContentParser;

public interface GeometryFormatFactory<ParsedFormat> {
    /**
     * Constructs a geometry format with specified characteristics
     * @param rightOrientation true if parser should assume the right orientation
     * @param coerce tru if parser should be lenient and coerce coordinates
     * @param ignoreZValue true if parser should accept and ignore Z coordinates
     * @return a geometry format
     */
    GeometryFormat<ParsedFormat> get(boolean rightOrientation, boolean coerce, boolean ignoreZValue);

    /**
     * Returns the name of the geometry format
     * @return name of the geometry format
     */
    String name();

    /**
     * Returns true if the data parsed by this format can start with the given token
     * @param token token to test
     * @return true if this parser can be used on this token
     */
    boolean acceptsAsAFirstToken(XContentParser.Token token);
}
