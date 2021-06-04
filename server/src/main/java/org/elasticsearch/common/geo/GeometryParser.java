/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.plugins.spi.GeometryFormatFactoryProvider;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * An utility class with a geometry parser methods supporting different shape representation formats
 */
public final class GeometryParser {

    private static final Logger logger = LogManager.getLogger(GeometryParser.class);

    private static volatile Map<String, GeometryFormatFactory<Geometry>> factories;

    static {
        factories = Collections.emptyMap();
        reloadFactories(GeometryParser.class.getClassLoader());
    }


    private final GeoJson geoJsonParser;
    private final WellKnownText wellKnownTextParser;
    private final boolean rightOrientation;
    private final boolean coerce;
    private final boolean ignoreZValue;

    public GeometryParser(boolean rightOrientation, boolean coerce, boolean ignoreZValue) {
        GeometryValidator validator = new StandardValidator(ignoreZValue);
        geoJsonParser = new GeoJson(rightOrientation, coerce, validator);
        wellKnownTextParser = new WellKnownText(coerce, validator);
        this.rightOrientation = rightOrientation;
        this.coerce = coerce;
        this.ignoreZValue = ignoreZValue;
    }

    /**
     * Parses supplied XContent into Geometry
     */
    public Geometry parse(XContentParser parser) throws IOException, ParseException {
        return geometryFormat(parser).fromXContent(parser);
    }

    /**
     * Returns a geometry format object that can parse and then serialize the object back to the same format.
     */
    public GeometryFormat<Geometry> geometryFormat(String format) {
        if (format.equals(GeoJsonGeometryFormat.NAME)) {
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else if (format.equals(WKTGeometryFormat.NAME)) {
            return new WKTGeometryFormat(wellKnownTextParser);
        } else {
            GeometryFormatFactory<Geometry> factory = factories.get(format);
            if (factory != null) {
                return factory.get(rightOrientation, coerce, ignoreZValue);
            }
            throw new IllegalArgumentException("Unrecognized geometry format [" + format + "].");
        }
    }

    /**
     * Returns a geometry format object that can parse and then serialize the object back to the same format.
     * This method automatically recognizes the format by examining the provided {@link XContentParser}.
     */
    public GeometryFormat<Geometry> geometryFormat(XContentParser parser) {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new WKTGeometryFormat(wellKnownTextParser);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            // We don't know the format of the original geometry - so going with default
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else {
            for (GeometryFormatFactory<Geometry> factory : factories.values()) {
                if (factory.acceptsAsAFirstToken(parser.currentToken())) {
                    return factory.get(rightOrientation, coerce, ignoreZValue);
                }
            }
            throw new ElasticsearchParseException("shape must be an object consisting of type and coordinates");
        }
    }

    /**
     * Parses the value as a {@link Geometry}. The following types of values are supported:
     * <p>
     * Object: has to contain either lat and lon or geohash fields
     * <p>
     * String: expected to be in "latitude, longitude" format, a geohash or WKT
     * <p>
     * Array: two or more elements, the first element is longitude, the second is latitude, the rest is ignored if ignoreZValue is true
     * <p>
     * Json structure: valid geojson definition
     */
    public  Geometry parseGeometry(Object value) throws ElasticsearchParseException {
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            if (values.size() == 2 && values.get(0) instanceof Number) {
                GeoPoint point = GeoUtils.parseGeoPoint(values, ignoreZValue);
                return new Point(point.lon(), point.lat());
            } else {
                List<Geometry> geometries = new ArrayList<>(values.size());
                for (Object object : values) {
                    geometries.add(parseGeometry(object));
                }
                return new GeometryCollection<>(geometries);
            }
        }
        try (XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            Collections.singletonMap("null_value", value), null)) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            if (isPoint(value)) {
                GeoPoint point = GeoUtils.parseGeoPoint(parser, new GeoPoint(), ignoreZValue);
                return new Point(point.lon(), point.lat());
            } else {
                return parse(parser);
            }

        } catch (IOException | ParseException ex) {
            throw new ElasticsearchParseException("error parsing geometry ", ex);
        }
    }

    private boolean isPoint(Object value) {
        // can we do this better?
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            return map.containsKey("lat") && map.containsKey("lon");
        } else if (value instanceof String) {
            String string = (String) value;
            return Character.isDigit(string.charAt(0)) || string.indexOf('(') == -1;
        }
        return false;
    }

    private static List<GeometryFormatFactory<Geometry>> getFactories(ClassLoader classLoader) {
        List<GeometryFormatFactory<Geometry>> factories = new ArrayList<>();
        ServiceLoader<GeometryFormatFactoryProvider> loader = ServiceLoader.load(GeometryFormatFactoryProvider.class, classLoader);
        for (GeometryFormatFactoryProvider provider : loader) {
            factories.addAll(provider.getGeometryFormatFactories());
        }
        return factories;
    }

    public synchronized static void reloadFactories(ClassLoader loader) {
        Map<String, GeometryFormatFactory<Geometry>> factoriesByName = new HashMap<>(factories);
        for (GeometryFormatFactory<Geometry> factory : getFactories(loader)) {
            if (factoriesByName.containsKey(factory.name()) == false) {
                logger.info("Registering geometry format factory for [{}]", factory.name());
                factoriesByName.put(factory.name(), factory);
            }
        }
        factories = Collections.unmodifiableMap(factoriesByName);
    }
}
