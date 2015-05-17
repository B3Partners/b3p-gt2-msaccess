package nl.b3p.geotools.data.msaccess.client;

import nl.b3p.geotools.data.msaccess.*;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 *
 * @author Chris
 */
public class Client {

    private static final Log log = LogFactory.getLog(Client.class);
    static final Logging logging = Logging.ALL;
    

    static {
        try {
            logging.setLoggerFactory("org.geotools.util.logging.CommonsLoggerFactory");
        } catch (ClassNotFoundException commonsException) {
            log.error("No commons logging for geotools");
            try {
                logging.setLoggerFactory("org.geotools.util.logging.Log4JLoggerFactory");
            } catch (ClassNotFoundException log4jException) {
                log.error("No logging at all for geotools");
            }
        }
    }
    /**
     * 
     * FOR TESTING
     * 
     * @param args
     * @throws java.lang.Exception
     */
    private static String FILE_EXT = null;

    public static void main(String[] args) throws Exception {

        Class c = Client.class;
        URL log4j_url = c.getResource("/nl/b3p/geotools/data/msaccess/log4j.properties");
        Properties p = new Properties();
        p.load(log4j_url.openStream());
        PropertyConfigurator.configure(p);
        log.info("logging configured!");

        Properties p2 = new Properties();
        URL dbconfig_url = c.getResource("/nl/b3p/geotools/data/msaccess/dbconfig.properties");
        p2.load(dbconfig_url.openStream());

        Map dbconfig = new HashMap();
        dbconfig.put(MsAccessDataStoreFactory.PARAM_URL.key, p2.getProperty("url", "C:/dev/OBIS.mdb"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_CONTROLER_TABLE.key, p2.getProperty("controlerTable", "_TABLES"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_CONTROLER_COLUMN_NAME.key, p2.getProperty("controlerColumnName", "TABLENAME"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_CONTROLER_COLUMN_TYPE.key, p2.getProperty("controlerColumnType", "TABLETYPE"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_CONTROLER_FILTER.key, p2.getProperty("controlerFilter", "[A-Z]%"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_CONTROLER_FILTER_REVERSE.key, p2.getProperty("controlerFilterReverse", "false"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_SRS.key, p2.getProperty("srs", "EPSG:28992"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_XLABELS.key, p2.getProperty("xlabels", "X_ORDINAAT"));
        dbconfig.put(MsAccessDataStoreFactory.PARAM_YLABELS.key, p2.getProperty("ylabels", "Y_ORDINAAT"));

        DataStore dataStore2Read = new MsAccessDataStore(dbconfig);
        if (dataStore2Read == null) {
            log.fatal("Problem with datastore to load. Datastore = null.");
            return;
        }

        String[] typeNames2Read = dataStore2Read.getTypeNames();
        for (int j = 0; j < 5; j++) {
            String typeName2Read = typeNames2Read[j];
            log.info("Reading: " + typeName2Read);

            FeatureSource features2Read = dataStore2Read.getFeatureSource(typeName2Read);
            FeatureCollection fc = features2Read.getFeatures();

            int loop=0;
            FeatureIterator fit = fc.features();
            while (fit.hasNext()) {
                SimpleFeature f = (SimpleFeature)fit.next();
                loop++;

                log.info("feature " + f.getIdentifier().getID());
                SimpleFeatureType ft = (SimpleFeatureType)f.getType();

                for(AttributeDescriptor descriptor : ft.getAttributeDescriptors()){
                    log.info(descriptor.getName().getLocalPart() + ": "+ f.getAttribute(descriptor.getName()));
                }
            }
            log.info("number of features: " + loop);
        }

    }
}
