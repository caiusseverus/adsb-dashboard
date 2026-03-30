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

typedef struct {
    uint8_t  msg_type;        /* Beast frame type: 0x31/0x32/0x33 */
    uint64_t timestamp;       /* 48-bit Beast timestamp */
    uint8_t  signal;          /* Beast RSSI byte */
    uint8_t  msg_len;         /* 2, 7, or 14 payload bytes */
    uint8_t  payload[14];     /* unescaped payload bytes */
} beast_frame_t;

typedef struct {
    uint8_t  data[65536];
    uint32_t len;
} beast_parser_t;

/* ── API ────────────────────────────────────────────────────────────────── */

/* Call once at process start.  Thread-safe after initialisation. */
void decode_init(void);

/* CPR position solving — thin wrappers over cpr.c */

/* Global (airborne) CPR: requires an even+odd frame pair.
 * fflag = 0 if the most-recently-received frame is even, 1 if odd.
 * Returns 0 on success (out_lat/out_lon populated), -1 on failure. */
int solve_cpr_airborne(int even_cprlat, int even_cprlon,
                       int odd_cprlat,  int odd_cprlon,
                       int fflag,
                       double *out_lat, double *out_lon);

/* Local (relative) CPR: single frame decoded against a reference position.
 * surface = 0 for airborne, 1 for surface movement.
 * Returns 0 on success, -1 on failure. */
int solve_cpr_relative(double reflat,  double reflon,
                       int cprlat,     int cprlon,
                       int fflag,      int surface,
                       double *out_lat, double *out_lon);

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

/* Stateful Beast stream parser.
 *
 * parser      : persistent parser state (zero-init once, then reuse)
 * chunk       : newly-read TCP bytes
 * chunk_len   : number of bytes in chunk
 * out_frames  : destination array for parsed frames
 * max_frames  : capacity of out_frames
 * malformed_bytes : optional output; incremented by bytes discarded during resync
 *
 * Appends chunk bytes to the parser buffer, extracts up to max_frames complete
 * frames, compacts any remainder in-place, and returns the number of frames
 * written to out_frames. Mode-A/C frames are included; caller may skip them.
 */
void beast_parser_init(beast_parser_t *parser);
int  beast_parse_chunk(beast_parser_t *parser,
                       const uint8_t *chunk, uint32_t chunk_len,
                       beast_frame_t *out_frames, int max_frames,
                       uint32_t *malformed_bytes);
