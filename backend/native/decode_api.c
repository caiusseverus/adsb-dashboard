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
#include "cpr.h"            /* decodeCPRairborne(), decodeCPRrelative() */

#include <string.h>

static int beast_msg_len(uint8_t msg_type) {
    switch (msg_type) {
        case 0x31: return 2;
        case 0x32: return 7;
        case 0x33: return 14;
        default:   return -1;
    }
}

static int beast_unescape_frame(const uint8_t *buf, uint32_t buf_len, uint32_t start,
                                int needed, beast_frame_t *frame, uint32_t *end_pos) {
    uint32_t pos = start;
    int out = 0;
    while (out < needed) {
        uint8_t b;
        if (pos >= buf_len) return 0; /* need more data */
        b = buf[pos];
        if (b == 0x1A) {
            if ((pos + 1) >= buf_len) return 0; /* partial escape pair */
            if (buf[pos + 1] != 0x1A) return -1; /* framing error */
            b = 0x1A;
            pos += 2;
        } else {
            pos += 1;
        }

        if (out < 6) {
            frame->timestamp = (frame->timestamp << 8) | b;
        } else if (out == 6) {
            frame->signal = b;
        } else {
            frame->payload[out - 7] = b;
        }
        out += 1;
    }

    frame->msg_len = (uint8_t)(needed - 7);
    *end_pos = pos;
    return 1;
}

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
    {
        double amplitude = (double)signal / 255.0;
        mm.signalLevel = amplitude * amplitude; /* Beast amplitude byte → normalized power */
    }
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

    /* ── DF11 interrogator identifier ── */
    if (mm.msgtype == 11) {
        result->iid = (int)mm.IID;
    }

    return 0;
}

void beast_parser_init(beast_parser_t *parser) {
    if (!parser) return;
    memset(parser, 0, sizeof(*parser));
}

int beast_parse_chunk(beast_parser_t *parser,
                      const uint8_t *chunk, uint32_t chunk_len,
                      beast_frame_t *out_frames, int max_frames,
                      uint32_t *malformed_bytes)
{
    uint32_t malformed = 0;
    uint32_t pos = 0;
    int out_count = 0;

    if (!parser || !out_frames || max_frames <= 0) return 0;

    if (chunk && chunk_len) {
        if ((uint64_t)parser->len + (uint64_t)chunk_len > sizeof(parser->data)) {
            parser->len = 0; /* overflow: drop buffered data and resync fresh */
        }
        if ((uint64_t)parser->len + (uint64_t)chunk_len <= sizeof(parser->data)) {
            memcpy(parser->data + parser->len, chunk, chunk_len);
            parser->len += chunk_len;
        } else {
            malformed += chunk_len;
        }
    }

    while ((pos + 2) <= parser->len && out_count < max_frames) {
        uint32_t end_pos = 0;
        int needed;
        uint8_t msg_type;
        beast_frame_t frame;

        if (parser->data[pos] != 0x1A) {
            uint32_t idx = pos + 1;
            while (idx < parser->len && parser->data[idx] != 0x1A) idx++;
            malformed += idx - pos;
            pos = idx;
            continue;
        }

        msg_type = parser->data[pos + 1];
        needed = beast_msg_len(msg_type);
        if (needed < 0) {
            malformed += 1;
            pos += 1;
            continue;
        }

        memset(&frame, 0, sizeof(frame));
        frame.msg_type = msg_type;
        switch (beast_unescape_frame(parser->data, parser->len, pos + 2, 7 + needed, &frame, &end_pos)) {
            case 0:
                goto done;
            case -1:
                malformed += 1;
                pos += 1;
                continue;
            default:
                out_frames[out_count++] = frame;
                pos = end_pos;
                continue;
        }
    }

done:
    if (pos > 0) {
        if (pos < parser->len) {
            memmove(parser->data, parser->data + pos, parser->len - pos);
        }
        parser->len -= pos;
    }
    if (malformed_bytes) *malformed_bytes = malformed;
    return out_count;
}

int solve_cpr_airborne(int even_cprlat, int even_cprlon,
                       int odd_cprlat,  int odd_cprlon,
                       int fflag,
                       double *out_lat, double *out_lon) {
    return decodeCPRairborne(even_cprlat, even_cprlon,
                             odd_cprlat,  odd_cprlon,
                             fflag, out_lat, out_lon);
}

int solve_cpr_relative(double reflat,  double reflon,
                       int cprlat,     int cprlon,
                       int fflag,      int surface,
                       double *out_lat, double *out_lon) {
    return decodeCPRrelative(reflat, reflon,
                             cprlat, cprlon,
                             fflag, surface,
                             out_lat, out_lon);
}
