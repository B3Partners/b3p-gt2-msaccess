package nl.b3p.geotools.data.msaccess;

import org.locationtech.jts.geom.Geometry;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class SpatialUtil {

    private static final Log log = LogFactory.getLog(SpatialUtil.class);

    static public AttributeDescriptor createAttributeType(String label, int sqlType) {
        AttributeDescriptor at = null;
        AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();
        attributeTypeBuilder.setName(label);

        if (sqlType == java.sql.Types.BIGINT ||
                sqlType == java.sql.Types.INTEGER ||
                sqlType == java.sql.Types.SMALLINT ||
                sqlType == java.sql.Types.TINYINT) {
            attributeTypeBuilder.setBinding(Long.class);

        } else if (sqlType == java.sql.Types.DATE ||
                sqlType == java.sql.Types.TIME ||
                sqlType == java.sql.Types.TIMESTAMP) {
            attributeTypeBuilder.setBinding(Timestamp.class);

        } else if (sqlType == java.sql.Types.DECIMAL ||
                sqlType == java.sql.Types.DOUBLE ||
                sqlType == java.sql.Types.FLOAT ||
                sqlType == java.sql.Types.REAL ||
                sqlType == java.sql.Types.NUMERIC) {
            attributeTypeBuilder.setBinding(Double.class);

        } else if (sqlType == java.sql.Types.CHAR ||
                sqlType == java.sql.Types.CLOB ||
                sqlType == java.sql.Types.LONGVARCHAR ||
                sqlType == java.sql.Types.VARCHAR) {
            attributeTypeBuilder.setBinding(String.class);

        } else if (sqlType == java.sql.Types.OTHER ||
                sqlType == java.sql.Types.BLOB) {
            attributeTypeBuilder.setBinding(Object.class);
        }


        return attributeTypeBuilder.buildDescriptor(label);
    }

    static public List getAttributeTypes(String typeName, Connection conn) throws Exception {
        DatabaseMetaData dbmd = conn.getMetaData();

        if (typeName == null) {
            return null;
        }
        ResultSet rs = dbmd.getColumns(null, null, typeName, null);
        List attributeTypes = null;
        while (rs.next()) {
            if (attributeTypes == null) {
                attributeTypes = new ArrayList();
            }
            String columnName = rs.getString("COLUMN_NAME");
            int sqlType = rs.getInt("DATA_TYPE");
            attributeTypes.add(createAttributeType(columnName, sqlType));
        }
        return attributeTypes;
    }

    static public SimpleFeatureType createFeatureType(String typeName, String epsg, Connection conn) throws Exception {

        List ats = getAttributeTypes(typeName, conn);
        if (ats == null || ats.isEmpty()) {
            return null;
        }
        CoordinateReferenceSystem crs = CRS.decode(epsg);

        AttributeTypeBuilder atb = new AttributeTypeBuilder();
        atb.setName("the_geom");
        atb.setCRS(crs);
        atb.setBinding(Geometry.class);
        atb.setNillable(true);

        ats.add(atb.buildDescriptor("the_geom"));

        AttributeDescriptor[] attributes = (AttributeDescriptor[]) ats.toArray(new AttributeDescriptor[ats.size()]);

        SimpleFeatureTypeBuilder ftb = new SimpleFeatureTypeBuilder();
        ftb.setName(typeName);
        ftb.setAttributes(attributes);

        return ftb.buildFeatureType();
    }

    static public List getTableNames(Connection conn) throws SQLException {
        DatabaseMetaData dbmd = conn.getMetaData();
        String[] types = new String[]{"TABLE", "VIEW"};
        ResultSet rs = dbmd.getTables(null, null, null, types);
        List tables = null;
        while (rs.next()) {
            String tableName = rs.getString("TABLE_NAME");
            if (tables == null) {
                tables = new ArrayList();
            }
            tables.add(tableName);
        }
        if (tables != null) {
            Collections.sort(tables);
        }
        return tables;
    }

    static public String[] getTableNameArray(Connection conn) throws SQLException {
        List tableList = SpatialUtil.getTableNames(conn);
        if (tableList == null || tableList.isEmpty()) {
            return null;
        }
        return (String[]) tableList.toArray(new String[tableList.size()]);
    }

    static public Map getLayerMap(
            String controlerTable,
            String controlerColumnName,
            String controlerColumnType,
            String controlerFilter,
            boolean controlerFilterReverse,
            Connection conn) throws SQLException {

        Map controlerMap = new TreeMap();
        StringBuffer q = new StringBuffer("select ");
        q.append(controlerColumnName);
        if (controlerColumnType != null) {
            q.append(", ");
            q.append(controlerColumnType);
        }
        q.append(" from ");
        q.append(controlerTable);
        if (controlerFilter != null) {
            q.append(" ct where ct.");
            q.append(controlerColumnName);
            if (controlerFilterReverse) {
                q.append(" not");
            }
            q.append(" like \'");
            q.append(controlerFilter);
            q.append("\'");
        }
        q.append(" ORDER BY ");
        q.append(controlerColumnName);

        log.debug("query: " + q.toString());

        try {
            PreparedStatement statement = conn.prepareStatement(q.toString());
            try {
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    if (controlerColumnType != null) {
                        controlerMap.put(rs.getString(controlerColumnName), rs.getString(controlerColumnType));
                    } else {
                        controlerMap.put(rs.getString(controlerColumnName), "");
                    }
                }
            } finally {
                statement.close();
            }
        } finally {
            try {
                conn.close();
            } catch (SQLException ex) {
                log.error("", ex);
            }
        }

        if (log.isDebugEnabled() && controlerMap != null) {
            Iterator it = controlerMap.keySet().iterator();
            while (it.hasNext()) {
                String tableName = (String) it.next();
                log.debug("table name:  " + tableName);
            }
        }

        return controlerMap;
    }

    static public String[] getLayerArray(
            String controlerTable,
            String controlerColumnName,
            String controlerColumnType,
            String controlerFilter,
            boolean controlerFilterReverse,
            Connection conn) throws SQLException {
        Map layerMap = SpatialUtil.getLayerMap(controlerTable, controlerColumnName,
                controlerColumnType, controlerFilter, controlerFilterReverse, conn);
        if (layerMap == null || layerMap.isEmpty()) {
            return null;
        }
        return (String[]) layerMap.keySet().toArray(new String[layerMap.size()]);
    }
}
