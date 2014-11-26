import geocode.ReverseGeoCode;
import twitter4j.GeoLocation;

import java.io.FileInputStream;
import java.io.IOException;

/**
 *  Provides Name-Lookups for GeoLocations
 */
public class LocationService {

    private static ReverseGeoCode geoCode;

    static {
        try {
            // Offline lookup requires a location file.
            geoCode = new ReverseGeoCode(new FileInputStream("locations.txt"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns nearest country of GeoLocation.
     * @param location
     * @return
     */
    public static String getCountry(GeoLocation location) {
        return geoCode.nearestPlace(location.getLatitude(), location.getLongitude()).country;
    }

    /**
     * Returns nearest identifiable location. Note:
     * the level of detail (e.g. major regions, cities, parts of cities)
     * depends on the file that initializes this class.
     * @param location
     * @return
     */
    public static String getNameForLocation(GeoLocation location) {
        String name = geoCode.nearestPlace(location.getLatitude(), location.getLongitude()).name;
        if (name.contains(".")) {
            name = name.replace("."," ");
        }

        return name;
    }
}
