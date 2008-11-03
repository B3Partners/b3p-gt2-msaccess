/*
 * $Id: MsAccessFeatureReader.java 8672 2008-07-17 16:37:57Z Matthijs $
 */
package nl.b3p.geotools.data.msaccess;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.geotools.data.FeatureReader;
import org.geotools.feature.Feature;
import org.geotools.feature.FeatureType;
import org.geotools.feature.IllegalAttributeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.feature.AttributeType;
import org.geotools.feature.GeometryAttributeType;
import org.geotools.referencing.NamedIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author Matthijs Laan, B3Partners
 */
public class MsAccessFeatureReader implements FeatureReader {

    private static final Log log = LogFactory.getLog(MsAccessFeatureReader.class);
    private FeatureType ft;
    private Connection conn;
    private ResultSet rs;
    private PreparedStatement statement;
    private GeometryFactory geometryFactory;
    private String[] xLabels;
    private String[] yLabels;
    public static final PrecisionModel precisionModel = new PrecisionModel(PrecisionModel.FLOATING);

    public MsAccessFeatureReader(FeatureType ft, String[] xLabels, String[] yLabels, Connection dbConn) throws IOException {
        this.conn = dbConn;
        this.ft = ft;
        this.xLabels = xLabels;
        this.yLabels = yLabels;
        GeometryAttributeType gat = ft.getDefaultGeometry();
        CoordinateReferenceSystem crs = gat.getCoordinateSystem();
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
        return ft;
    }

    public Feature next() throws IOException, IllegalAttributeException, NoSuchElementException {
        List foa = new ArrayList();
        int ac = ft.getAttributeCount();

        int xls = xLabels.length;
        int yls = yLabels.length;
        double x = 0.0;
        double y = 0.0;
        int addIndex = -1; // geom moet hier ingevoegd worden

        for (int i = 0; i < ac; i++) {
            AttributeType at = ft.getAttributeType(i);
            String name = at.getName();

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
                Class atc = at.getType();
                try {
                    if (atc == Long.class) {
                        foa.add(new Long(rs.getLong(name)));
                    } else if (atc == Date.class) {
                        foa.add(rs.getDate(name));
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
        }

        if (addIndex >= 0) {
            foa.add(addIndex, geometryFactory.createPoint(new Coordinate(x, y)));
        }

        Feature f = ft.create(foa.toArray());
        return f;
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
