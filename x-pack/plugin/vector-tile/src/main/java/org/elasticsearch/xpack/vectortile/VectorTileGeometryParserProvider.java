/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile;

import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryFormatFactory;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.plugins.spi.GeometryFormatFactoryProvider;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class VectorTileGeometryParserProvider implements GeometryFormatFactoryProvider {
    public static final String NAME = "vectortile";

    @Override
    public List<GeometryFormatFactory<Geometry>> getGeometryFormatFactories() {
        return List.of(new VectorTileGeometryFormatFactory());
    }

    private static class VectorTileGeometryFormatFactory implements GeometryFormatFactory<Geometry> {

        @Override
        public GeometryFormat<Geometry> get(boolean rightOrientation, boolean coerce, boolean ignoreZValue) {
            return new VectorTileGeometryFormat();
        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public boolean acceptsAsAFirstToken(XContentParser.Token token) {
            return false;
        }
    }

    private static class VectorTileGeometryFormat implements GeometryFormat<Geometry> {

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public Geometry fromXContent(XContentParser parser) throws IOException, ParseException {
            throw new IOException("Vector tile format doesn't support parsing");
        }

        @Override
        public XContentBuilder toXContent(Geometry geometry, XContentBuilder builder, ToXContent.Params params) throws IOException {
            // TODO: add real implementation
            return builder.value(getBytes(geometry));
        }

        @Override
        public Object toXContentAsObject(Geometry geometry) {
            return getBytes(geometry);
        }

        private byte[] getBytes(Geometry geometry) {
            // TODO: add real implementation
            return new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        }
    }

}
