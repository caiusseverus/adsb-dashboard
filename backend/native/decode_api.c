/*
 * decode_api.c — implementation of the public decode API.
 *
 * Wraps the readsb-derived decode pipeline (mode_s.c, crc.c, cpr.c,
 * comm_b.c, icao_filter.c, ais_charset.c) behind a simple C API that
 * cffi can call without touching any readsb internals.
 */

#include "decode_api.h"
#include "decode_types.h"   /* struct modesMessage, enums, Modes */
#include "mode_s.h"         /* decodeModesMessage()              */
#include "crc.h"            /* modesChecksumInit(), crcCleanupTables() */
#include "icao_filter.h"    /* icaoFilterInit(), icaoFilterDestroy() */

#include <string.h>

/* ── Global Modes stub ──────────────────────────────────────────────────── */

/*
 * mode_s.c and comm_b.c reference the global `Modes` struct for a handful
 * of flags (fixDF, nfix_crc, etc.).  We define it here with decode-only
 * values; everything else stays 0 (disabled).
 */
struct _decode_modes_t Modes = {
    .fixDF          = 1,   /* attempt to correct corrupted DF field */
    .nfix_crc       = 1,   /* correct up to 1 CRC bit error */
    .decode_all     = 0,
    .net_verbatim   = 0,
    .netIngest      = 0,
    .garbage_ports  = 0,
    .debug_callsign = 0,
    .onlyaddr       = 0,
    .mlat           = 0,
    .raw            = 0,
    .stats_current  = { .cpr_filtered = 0 },
};

/* ── API implementation ─────────────────────────────────────────────────── */

void decode_init(void) {
    modesChecksumInit(1);   /* build CRC tables; 1 = include 1-bit-fix table */
    icaoFilterInit();
}

void decode_cleanup(void) {
    crcCleanupTables();
    icaoFilterDestroy();
}

int decode_message(const uint8_t *msg_bytes, int msg_len,
                   uint8_t signal, uint64_t timestamp,
                   decode_result_t *result)
{
    if (!msg_bytes || !result) return -1;
    if (msg_len != MODES_SHORT_MSG_BYTES && msg_len != MODES_LONG_MSG_BYTES)
        return -1;

    /* Zero-initialise the output first so caller gets clean data on error */
    memset(result, 0, sizeof(*result));

    /* Build a modesMessage for the decoder */
    struct modesMessage mm;
    memset(&mm, 0, sizeof(mm));

    memcpy(mm.msg,      msg_bytes, (size_t)msg_len);
    memcpy(mm.verbatim, msg_bytes, (size_t)msg_len);

    mm.msgbits     = msg_len * 8;
    mm.signalLevel = (double)(255 - signal) / 255.0; /* Beast RSSI→0–1 */
    mm.timestamp   = (int64_t)timestamp;
    mm.remote      = 1;   /* treat as remote (network) frame — skips SDR bits */

    /* Decode */
    int rc = decodeModesMessage(&mm);
    if (rc < 0)
        return -1;   /* bad CRC / unknown DF / rejected */

    /*
     * decodeModesMessage() returns 0 on success, -1 on hard failure, and
     * positive values for Mode-A/C.  Accept anything >= 0.
     */

    /* ── Frame metadata ── */
    result->df            = mm.msgtype;
    result->addr          = mm.addr;
    result->correctedbits = mm.correctedbits;
    result->addrtype      = (int)mm.addrtype;

    /* ── Callsign ── */
    if (mm.callsign_valid) {
        result->callsign_valid = true;
        memcpy(result->callsign, mm.callsign, sizeof(result->callsign));
    }

    /* ── Altitude ── */
    if (mm.baro_alt_valid) {
        result->baro_alt_valid = true;
        result->baro_alt       = mm.baro_alt;
    }
    if (mm.geom_alt_valid) {
        result->geom_alt_valid = true;
        result->geom_alt       = mm.geom_alt;
    }
    result->airground = (int)mm.airground;

    /* ── Squawk ── */
    if (mm.squawk_valid) {
        result->squawk_valid = true;
        result->squawk       = mm.squawkHex;
    }

    /* ── CPR position ── */
    if (mm.cpr_valid) {
        result->cpr_valid = true;
        result->cpr_odd   = mm.cpr_odd;
        result->cpr_type  = (int)mm.cpr_type;
        result->cpr_lat   = mm.cpr_lat;
        result->cpr_lon   = mm.cpr_lon;
    }

    /* ── EHS: BDS 5,0 / 6,0 ── */
    if (mm.heading_valid) {
        result->heading_valid = true;
        result->heading       = mm.heading;
        result->heading_type  = (int)mm.heading_type;
    }
    if (mm.ias_valid) {
        result->ias_valid = true;
        result->ias       = mm.ias;
    }
    if (mm.tas_valid) {
        result->tas_valid = true;
        result->tas       = mm.tas;
    }
    if (mm.mach_valid) {
        result->mach_valid = true;
        result->mach       = mm.mach;
    }
    if (mm.baro_rate_valid) {
        result->baro_rate_valid = true;
        result->baro_rate       = mm.baro_rate;
    }

    /* ── Navigation intent: BDS 4,0 / TS&S ── */
    if (mm.nav.mcp_altitude_valid) {
        result->nav_altitude_mcp_valid = true;
        result->nav_altitude_mcp       = mm.nav.mcp_altitude;
    }
    if (mm.nav.fms_altitude_valid) {
        result->nav_altitude_fms_valid = true;
        result->nav_altitude_fms       = mm.nav.fms_altitude;
    }
    if (mm.nav.heading_valid) {
        result->nav_heading_valid = true;
        result->nav_heading       = mm.nav.heading;
    }
    if (mm.nav.modes_valid) {
        result->nav_modes_valid = true;
        result->nav_modes       = (unsigned)mm.nav.modes;
    }

    /* ── ACAS RA: BDS 3,0 ── */
    if (mm.acas_ra_valid) {
        result->acas_ra_valid = true;
        memcpy(result->MV, mm.MV, sizeof(result->MV));
    }

    /* ── Emitter category ── */
    if (mm.category_valid) {
        result->category_valid = true;
        result->category       = mm.category;
    }

    /* ── Emergency ── */
    if (mm.emergency_valid) {
        result->emergency_valid = true;
        result->emergency       = (int)mm.emergency;
    }

    return 0;
}
