/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.spi;

import org.elasticsearch.common.geo.GeometryFormatFactory;
import org.elasticsearch.geometry.Geometry;

import java.util.List;

/**
 * Provides alternative geometry parsers and generators
 */
public interface GeometryFormatFactoryProvider {
    List<GeometryFormatFactory<Geometry>>  getGeometryFormatFactories();
}
