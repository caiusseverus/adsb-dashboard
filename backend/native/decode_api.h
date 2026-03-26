/*
 * decode_api.h — public C API for the readsb-derived Mode-S decode library.
 *
 * Exposes a flat struct (decode_result_t) with all fields Python needs,
 * populated by decode_message() from a raw Beast payload.
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>

/* ── Result struct ──────────────────────────────────────────────────────── */

typedef struct {
    /* Frame metadata */
    int      df;              /* Downlink format (0–24, 77=Mode-A/C) */
    uint32_t addr;            /* 24-bit ICAO address */
    int      correctedbits;   /* 0 = clean CRC; 1 = 1-bit corrected */
    int      addrtype;        /* addrtype_t cast to int */

    /* Identification */
    bool     callsign_valid;
    char     callsign[16];    /* 8-char NUL-terminated flight ID */

    /* Altitude */
    bool     baro_alt_valid;
    int      baro_alt;        /* barometric altitude, feet */
    bool     geom_alt_valid;
    int      geom_alt;        /* geometric (GNSS) altitude, feet */
    int      airground;       /* airground_t: 0=invalid,1=ground,2=air,3=uncertain */

    /* Squawk */
    bool     squawk_valid;
    uint32_t squawk;          /* 4-digit hex-encoded squawk e.g. 0x7700 */

    /* CPR position */
    bool     cpr_valid;
    bool     cpr_odd;         /* true = odd frame */
    int      cpr_type;        /* cpr_type_t cast to int */
    uint32_t cpr_lat;
    uint32_t cpr_lon;

    /* EHS — BDS 5.0 / 6.0 */
    bool     heading_valid;
    float    heading;
    int      heading_type;    /* heading_type_t cast to int */
    bool     ias_valid;
    unsigned ias;             /* indicated airspeed, kts */
    bool     tas_valid;
    unsigned tas;             /* true airspeed, kts */
    bool     mach_valid;
    double   mach;
    bool     baro_rate_valid;
    int      baro_rate;       /* vertical rate, fpm */

    /* Navigation intent — BDS 4.0 / TS&S */
    bool     nav_altitude_mcp_valid;
    unsigned nav_altitude_mcp;
    bool     nav_altitude_fms_valid;
    unsigned nav_altitude_fms;
    bool     nav_heading_valid;
    float    nav_heading;
    bool     nav_modes_valid;
    unsigned nav_modes;       /* nav_modes_t bitmask */

    /* ACAS/TCAS RA */
    bool          acas_ra_valid;
    unsigned char MV[7];      /* raw BDS 3.0 payload for external ACAS decode */

    /* Emitter category */
    bool     category_valid;
    unsigned category;        /* e.g. 0xA1 */

    /* Emergency */
    bool     emergency_valid;
    int      emergency;       /* emergency_t cast to int */
} decode_result_t;

/* ── API ────────────────────────────────────────────────────────────────── */

/* Call once at process start.  Thread-safe after initialisation. */
void decode_init(void);

/* Release tables allocated by decode_init(). */
void decode_cleanup(void);

/*
 * Decode one Beast payload.
 *
 * msg_bytes : raw Mode-S bytes (7 for short, 14 for long)
 * msg_len   : 7 or 14
 * signal    : Beast RSSI byte (0=strongest, 255=weakest) — stored for info only
 * timestamp : 6-byte Beast timestamp (48-bit, 12 MHz clock)
 * result    : output struct; zeroed then populated on success
 *
 * Returns 0 on success (message decoded, result populated).
 * Returns -1 if the message was rejected (bad CRC, unknown DF, too short).
 */
int decode_message(const uint8_t *msg_bytes, int msg_len,
                   uint8_t signal, uint64_t timestamp,
                   decode_result_t *result);
