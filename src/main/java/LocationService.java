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
            geoCode = new ReverseGeoCode(new FileInputStream("cities1000.txt"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getCountry(GeoLocation location) {
        return geoCode.nearestPlace(location.getLatitude(), location.getLongitude()).country;
    }

    public static String getNameForLocation(GeoLocation location) {
        return geoCode.nearestPlace(location.getLatitude(), location.getLongitude()).name;
    }
}
