/*
 * $Id: MsAccessFeatureReader.java 8672 2008-07-17 16:37:57Z Matthijs $
 */
package nl.b3p.geotools.data.msaccess;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.geotools.data.FeatureReader;
import org.geotools.feature.IllegalAttributeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.NamedIdentifier;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.feature.type.FeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author Matthijs Laan, B3Partners
 */
public class MsAccessFeatureReader implements FeatureReader {

    private static final Log log = LogFactory.getLog(MsAccessFeatureReader.class);
    private SimpleFeatureType ft;
    private Connection conn;
    private ResultSet rs;
    private PreparedStatement statement;
    private GeometryFactory geometryFactory;
    private String[] xLabels;
    private String[] yLabels;
    private int featureCount;
    public static final PrecisionModel precisionModel = new PrecisionModel(PrecisionModel.FLOATING);

    public MsAccessFeatureReader(SimpleFeatureType ft, String[] xLabels, String[] yLabels, Connection dbConn) throws IOException {
        this.conn = dbConn;
        this.ft = ft;
        this.xLabels = xLabels;
        this.yLabels = yLabels;

        CoordinateReferenceSystem crs = ft.getGeometryDescriptor().getCoordinateReferenceSystem();
        int SRID = -1;
        if (crs != null) {
            try {
                Set ident = crs.getIdentifiers();
                if (ident != null && !ident.isEmpty()) {
                    String code = ((NamedIdentifier) ident.toArray()[0]).getCode();
                    SRID = Integer.parseInt(code);
                }
            } catch (Exception e) {
                log.error("SRID could not be determined from crs!");
            }
        }
        geometryFactory = new GeometryFactory(precisionModel, SRID);

        String q = "select * from " + ft.getTypeName();

        try {
            statement = conn.prepareStatement(q);
            rs = statement.executeQuery();
        } catch (SQLException ex) {
            throw new IOException(ex.getLocalizedMessage());
        }
    }

    public FeatureType getFeatureType() {
        return (FeatureType)ft;
    }

    public Feature next() throws IOException, IllegalAttributeException, NoSuchElementException {
        List foa = new ArrayList();

        int xls = xLabels.length;
        int yls = yLabels.length;
        double x = 0.0;
        double y = 0.0;
        int addIndex = -1; // geom moet hier ingevoegd worden

        //for (int i = 0; i < ac; i++) {
        int i = 0;
        for(AttributeDescriptor attributeDescriptor : ft.getAttributeDescriptors()){
            String name = attributeDescriptor.getName().getLocalPart();

            boolean isXLabel = false;
            boolean isYLabel = false;
            if (xls > 0 && yls > 0) {
                for (int ix = 0; ix < xls; ix++) {
                    if (name.equals(xLabels[ix].trim())) {
                        isXLabel = true;
                        break;
                    }
                }
                for (int iy = 0; iy < yls; iy++) {
                    if (name.equals(yLabels[iy].trim())) {
                        isYLabel = true;
                        break;
                    }
                }
            }

            if ("the_geom".equalsIgnoreCase(name)) {
                addIndex = i;
            } else {
                Class atc = attributeDescriptor.getType().getBinding();
                try {
                    if (atc == Long.class) {
                        foa.add(new Long(rs.getLong(name)));
                    } else if (atc == Timestamp.class) {
                        Timestamp ts = rs.getTimestamp(name);
                        foa.add(ts);
                    } else if (atc == Double.class) {
                        Double val = new Double(rs.getDouble(name));
                        if (val!=null && isXLabel) {
                            x = val.doubleValue();
                        }
                        if (val!=null && isYLabel) {
                            y = val.doubleValue();
                        }
                        foa.add(val);
                    } else if (atc == String.class) {
                        foa.add(rs.getString(name));
                    } else if (atc == Object.class) {
                        foa.add(rs.getObject(name));
                    } else {
                        foa.add(new Object());
                    }
                } catch (SQLException ex) {
                    throw new NoSuchElementException(ex.getMessage());
                }
            }
            i++;
        }

        if (addIndex >= 0) {
            foa.add(addIndex, geometryFactory.createPoint(new Coordinate(x, y)));
        }

        SimpleFeatureBuilder fb = new SimpleFeatureBuilder(ft);
        return (Feature)fb.build(ft, foa, Integer.toString(featureCount++));
    }

    public boolean hasNext() throws IOException {
        try {
            return rs.next();
        } catch (SQLException ex) {
            throw new IOException(ex.getLocalizedMessage());
        }
    }

    public void close() throws IOException {
        try {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException ex) {
            throw new IOException(ex.getLocalizedMessage());
        }

    }
}
