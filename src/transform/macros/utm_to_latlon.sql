{% macro create_utm_to_latlon_udf() %}

    create or replace function {{ target.schema }}.utm_to_latlon(
        easting float64, northing float64, zone int64
    )
    returns struct<latitude float64, longitude float64>
    language js
    as r"""
        // Inverse Transverse Mercator projection (WGS84, Southern Hemisphere)
        // Based on Krüger series expansion — sub-meter accuracy
        if (easting == null || northing == null || zone == null) return null;

        // WGS84 ellipsoid constants
        const a = 6378137.0;
        const f = 1 / 298.257223563;
        const b = a * (1 - f);
        const e = Math.sqrt(1 - (b * b) / (a * a));
        const e2 = e * e;
        const ep2 = e2 / (1 - e2);  // e'^2
        const k0 = 0.9996;

        // Remove false easting/northing (Southern Hemisphere)
        const x = easting - 500000.0;
        const y = northing - 10000000.0;

        // Central meridian for the zone
        const lon0 = ((zone - 1) * 6 - 180 + 3) * Math.PI / 180;

        // Meridional arc constants
        const n = (a - b) / (a + b);
        const n2 = n * n;
        const n3 = n * n2;
        const n4 = n * n3;

        // Rectifying radius
        const A = (a / (1 + n)) * (1 + n2 / 4 + n4 / 64);

        // Krüger series coefficients (inverse: from projection to geodetic)
        const beta1 = n / 2 - (2 / 3) * n2 + (37 / 96) * n3 - (1 / 360) * n4;
        const beta2 = (1 / 48) * n2 + (1 / 15) * n3 - (437 / 1440) * n4;
        const beta3 = (17 / 480) * n3 - (37 / 840) * n4;
        const beta4 = (4397 / 161280) * n4;

        // Normalized coordinates
        const xi = y / (k0 * A);
        const eta = x / (k0 * A);

        // Inverse Krüger series
        let xi_prime = xi;
        let eta_prime = eta;
        for (let j = 1; j <= 4; j++) {
            const beta = [0, beta1, beta2, beta3, beta4][j];
            xi_prime -= beta * Math.sin(2 * j * xi) * Math.cosh(2 * j * eta);
            eta_prime -= beta * Math.cos(2 * j * xi) * Math.sinh(2 * j * eta);
        }

        // Conformal latitude
        const chi = Math.asin(Math.sin(xi_prime) / Math.cosh(eta_prime));

        // Geodetic latitude from conformal latitude (series expansion)
        const delta1 = 2 * n - (2 / 3) * n2 - 2 * n3;
        const delta2 = (7 / 3) * n2 - (8 / 5) * n3;
        const delta3 = (56 / 15) * n3;

        let lat = chi;
        lat += delta1 * Math.sin(2 * chi);
        lat += delta2 * Math.sin(4 * chi);
        lat += delta3 * Math.sin(6 * chi);

        // Geodetic longitude
        const lon = lon0 + Math.atan2(
            Math.sinh(eta_prime),
            Math.cos(xi_prime)
        );

        // Convert to degrees
        const latitude = lat * 180 / Math.PI;
        const longitude = lon * 180 / Math.PI;

        if (isNaN(latitude) || isNaN(longitude)) {
            return null;
        }

        return {
            latitude: latitude,
            longitude: longitude
        };
    """;

{% endmacro %}
