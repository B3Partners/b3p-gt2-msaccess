/*
 * $Id: MsAccessDataStore.java 8672 2008-07-17 16:37:57Z Matthijs $
 */
package nl.b3p.geotools.data.msaccess;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.data.DefaultServiceInfo;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.FileDataStore;
import org.geotools.data.LockingManager;
import org.geotools.data.Query;
import org.geotools.data.ServiceInfo;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;

/**
 * DataStore for reading a DXF file produced by Autodesk.
 *
 * The attributes are always the same: key: String name: String urlLink: String
 * entryLineNumber: Integer parseError: Boolean error: String
 *
 * @author Chris van Lith B3Partners
 * @author mprins
 */
public class MsAccessDataStore implements FileDataStore {

    private static final Log log = LogFactory.getLog(MsAccessDataStore.class);
    private URL url;
    private Connection dbConn;
    private String controlerTable = null;
    private String controlerColumnName = null;
    private String controlerColumnType = null;
    private String controlerFilter = null;
    private boolean controlerFilterReverse = false;
    private String epsg = null;
    private String[] xLabels = null;
    private String[] yLabels = null;

    private Map featureReaderMap = new HashMap();
    private Map featureTypeMap = new HashMap();

    public MsAccessDataStore(URL url) throws IOException {
        this.url = url;
        dbConn = getConnection();
    }

    protected Connection getConnection() throws IOException {
        return getConnection(url);
    }

    protected static Connection getConnection(URL url) throws IOException {
        String msaccessFile = url.getFile().toLowerCase().substring(1);
        try {
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver").newInstance();
        } catch (InstantiationException ex) {
            log.error("JdbcOdbcDriver not found!", ex);
            throw new IOException(ex.getLocalizedMessage());
        } catch (IllegalAccessException ex) {
            log.error("JdbcOdbcDriver not found!", ex);
            throw new IOException(ex.getLocalizedMessage());
        } catch (ClassNotFoundException ex) {
            log.error("JdbcOdbcDriver not found!", ex);
            throw new IOException(ex.getLocalizedMessage());
        }
        String myDB = "jdbc:odbc:Driver={Microsoft Access Driver (*.mdb)};DBQ=" + msaccessFile;
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(myDB, "yuit", "tyuityu");
        } catch (SQLException ex) {
            log.error("Connectionstring: " + myDB);
            log.error("Connection not made!", ex);
            throw new IOException(ex.getLocalizedMessage());
        }

//        Properties p = new Properties();
//        p.put("test", "DRIVER={SQL Server};ServerName=itrackx;UID=defuser;PWD=password");
//        String sConnect = new String("jdbc:itrackx://192.233.0.3/,");
//        Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
//        conn = DriverManager.getConnection(sConnect, p);
//Problem: The account that is being used to access the page does not have access 
//        to the HKEY_LOCAL_MACHINE\SOFTWARE\ODBC registry key.
//
//Solution:
//http://support.microsoft.com/default.aspx?scid=kb;EN-US;Q295297
        return conn;
    }

    public MsAccessDataStore(Map dbconfig) throws IOException {
        this((URL) dbconfig.get(MsAccessDataStoreFactory.PARAM_URL.key));
        controlerTable = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_CONTROLER_TABLE.key);
        controlerColumnName = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_CONTROLER_COLUMN_NAME.key);
        controlerColumnType = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_CONTROLER_COLUMN_TYPE.key);
        controlerFilter = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_CONTROLER_FILTER.key);
        String reverse = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_CONTROLER_FILTER_REVERSE.key);
        if (reverse != null && reverse.equalsIgnoreCase("true")) {
            controlerFilterReverse = true;
        }
        epsg = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_SRS.key);
        String xlbs = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_XLABELS.key);
        String ylbs = (String) dbconfig.get(MsAccessDataStoreFactory.PARAM_YLABELS.key);
        xLabels = xlbs.split(",");
        yLabels = ylbs.split(",");
    }

    public String[] getTypeNames() throws IOException {
        try {
            if (controlerTable == null || controlerColumnName == null) {
                return SpatialUtil.getTableNameArray(dbConn);
            }
            return SpatialUtil.getLayerArray(controlerTable, controlerColumnName,
                    controlerColumnType, controlerFilter, controlerFilterReverse, dbConn);
        } catch (SQLException ex) {
            throw new IOException(ex.getLocalizedMessage());
        }
    }

    public SimpleFeatureType getSchema(String typeName) throws IOException {
        if (featureTypeMap.containsKey(typeName)) {
            return (SimpleFeatureType) featureTypeMap.get(typeName);
        }
        try {
            // TODO raar dat dit nodig is
            if (dbConn.isClosed()) {
                dbConn = getConnection();
            }

            SimpleFeatureType ft = SpatialUtil.createFeatureType(typeName, epsg, dbConn);
            featureTypeMap.put(typeName, ft);
            return ft;
        } catch (Exception ex) {
            throw new IOException(ex.getLocalizedMessage());
        }
    }

    @Override
    public SimpleFeatureType getSchema(Name name) throws IOException {
        return getSchema(name.getLocalPart());
    }

    @Override
    public SimpleFeatureType getSchema() throws IOException {
        return null;
    }

    @Override
    public List<Name> getNames() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public FeatureReader getFeatureReader(String typeName) throws IOException {
        if (featureReaderMap.containsKey(typeName)) {
            return (FeatureReader) featureReaderMap.get(typeName);
        }
        try {
            FeatureReader fr = new MsAccessFeatureReader(getSchema(typeName), xLabels, yLabels, dbConn);
            featureReaderMap.put(typeName, fr);
            return fr;
        } catch (Exception ex) {
            throw new IOException(ex.getLocalizedMessage());
        }
    }

    @Override
    public FeatureReader getFeatureReader() throws IOException {
        return null;
    }

    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(Query query, Transaction t) throws IOException {
        return getFeatureReader(query.getTypeName());
    }

    @Override
    public SimpleFeatureSource getFeatureSource() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SimpleFeatureSource getFeatureSource(String string) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SimpleFeatureSource getFeatureSource(Name name) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void createSchema(SimpleFeatureType t) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void updateSchema(Name name, SimpleFeatureType t) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void updateSchema(SimpleFeatureType sft) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void updateSchema(String string, SimpleFeatureType sft) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeSchema(String string) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeSchema(Name name) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Filter filter, Transaction t) throws IOException {
        throw new UnsupportedOperationException("Functie niet ondersteund voor alleen-lezen databron.");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(Transaction t) throws IOException {
        throw new UnsupportedOperationException("Functie niet ondersteund voor alleen-lezen databron.");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(Transaction t) throws IOException {
        throw new UnsupportedOperationException("Functie niet ondersteund voor alleen-lezen databron.");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String string, Filter filter, Transaction t) throws IOException {
        throw new UnsupportedOperationException("Functie niet ondersteund voor alleen-lezen databron.");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriter(String string, Transaction t) throws IOException {
        throw new UnsupportedOperationException("Functie niet ondersteund voor alleen-lezen databron.");
    }

    @Override
    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFeatureWriterAppend(String string, Transaction t) throws IOException {
        throw new UnsupportedOperationException("Functie niet ondersteund voor alleen-lezen databron.");
    }

    @Override
    public LockingManager getLockingManager() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ServiceInfo getInfo() {
        DefaultServiceInfo serviceInfo = new DefaultServiceInfo();
        serviceInfo.setTitle("MSAccess DataStore");
        try {
            serviceInfo.setSource(this.url.toURI());
        } catch (URISyntaxException ex) {

        }
        return serviceInfo;
    }

    @Override
    public void dispose() {
        try {
            this.dbConn.close();
        } catch (SQLException | NullPointerException ex) {
            log.debug("Mogelijk probleem met sluiten van DB verbinding", ex);
        }
        this.dbConn = null;
    }
}
