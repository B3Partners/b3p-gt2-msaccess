/*
 * $Id: MsAccessDataStoreFactory.java 8672 2008-07-17 16:37:57Z Matthijs $
 */

package nl.b3p.geotools.data.msaccess;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFactorySpi;

/**
 * @author Matthijs Laan, B3Partners
 */
public class MsAccessDataStoreFactory implements FileDataStoreFactorySpi {
    private static final Log log = LogFactory.getLog(MsAccessDataStoreFactory.class);

    public static final DataStoreFactorySpi.Param PARAM_URL = new Param("url", URL.class, "url to a .dxf file");    
    public static final DataStoreFactorySpi.Param PARAM_CONTROLER_TABLE = new Param("controlerTable", String.class, "table in mdb that holds tables names that point to layers");  
    public static final DataStoreFactorySpi.Param PARAM_CONTROLER_COLUMN_NAME = new Param("controlerColumnName", String.class, "column name in controller tabel that holds table names");  
    public static final DataStoreFactorySpi.Param PARAM_CONTROLER_COLUMN_TYPE = new Param("controlerColumnType", String.class, "column name in controller tabel that holds table types");  
    public static final DataStoreFactorySpi.Param PARAM_CONTROLER_FILTER = new Param("controlerFilter", String.class, "sql like filter to apply to controler table");  
    public static final DataStoreFactorySpi.Param PARAM_CONTROLER_FILTER_REVERSE = new Param("controlerFilterReverse", Boolean.class, "if true use not like insteadof like");  
    public static final DataStoreFactorySpi.Param PARAM_SRS = new Param("srs", String.class, "EPSG code of projection, e.g. EPSG:28992");  
    public static final DataStoreFactorySpi.Param PARAM_XLABELS = new Param("xlabels", String.class, "comma separated list of column names with x coordinates (one per type)");  
    public static final DataStoreFactorySpi.Param PARAM_YLABELS = new Param("ylabels", String.class, "comma separated list of column names with y coordinates (one per type)");  
    
    public String getDisplayName() {
        return "MS Access database";
    }

    public String getDescription() {
        return "MS Access database";
    }

    public String[] getFileExtensions() {
        return new String[] {".mdb"};
    }

    /**
     * @return true if the file of the f parameter exists
     */
    public boolean canProcess(URL f) {
        return f.getFile().toLowerCase().endsWith(".mdb");  
    }

    /**
     * @return true if the file in the url param exists
     */
    public boolean canProcess(Map params) {
        boolean result = false;
        if (params.containsKey(PARAM_URL.key)) {
            try {
                URL url = (URL)PARAM_URL.lookUp(params);
                result = canProcess(url);
            } catch (IOException ioe) {
                /* return false on any exception */
            }
        }
        return result;
    }

    /*
     * Always returns true, no additional libraries needed
     */
    public boolean isAvailable() {
        return true;
    }

    public Param[] getParametersInfo() {
        return new Param[] {PARAM_URL};
    }

    public Map getImplementationHints() {
        /* XXX do we need to put something in this map? */
        return Collections.EMPTY_MAP;
    }

    public String getTypeName(URL url) throws IOException {
        return null;
    }

    public FileDataStore createDataStore(URL url) throws IOException {
        Map params = new HashMap();
        params.put(PARAM_URL.key, url);
        
        boolean isLocal = url.getProtocol().equalsIgnoreCase("file");
        if(isLocal && !(new File(url.getFile()).exists())){
            throw new UnsupportedOperationException("Specified MS Access database \"" + url + "\" does not exist, this plugin is read-only so no new file will be created");
        } else {
            return createDataStore(params);
        }        
    }

    public FileDataStore createDataStore(Map params) throws IOException {
        if(!canProcess(params)) {
            throw new FileNotFoundException( "MS Access database not found: " + params);
        }
        //return new MsAccessDataStore((URL)params.get(PARAM_URL.key));
        return new MsAccessDataStore(params);
    }

    public DataStore createNewDataStore(Map params) throws IOException {
        throw new UnsupportedOperationException("This plugin is read-only");
    }
}