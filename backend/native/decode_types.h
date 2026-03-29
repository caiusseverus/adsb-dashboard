/*
 * decode_types.h — minimal replacement for readsb.h for the extracted decode
 * modules (crc.c, cpr.c, mode_s.c, comm_b.c, icao_filter.c, ais_charset.c).
 *
 * Covers only what those files actually reference.  The full readsb.h is
 * 1400+ lines covering SDR, networking, globe indexing, etc. — none of
 * which is needed for stand-alone Mode-S decoding.
 *
 * License note: the extracted C files are GPLv3+ (readsb project).
 * This header is original work and may be used under any licence.
 */

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdio.h>
#include <time.h>

/* ── Compiler helpers ───────────────────────────────────────────────────── */

#define likely(x)   __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

#define MemoryAlignment 32
#define ALIGNED __attribute__((aligned(MemoryAlignment)))

#if defined(__GNUC__) && __GNUC__ >= 8
#  define _unroll_8  _Pragma("GCC unroll 8")
#  define _unroll_16 _Pragma("GCC unroll 16")
#  define _unroll_32 _Pragma("GCC unroll 32")
#else
#  define _unroll_8
#  define _unroll_16
#  define _unroll_32
#endif

/* ── Memory helpers (simple wrappers — no exit-on-failure for library use) */

static inline void *_decode_malloc(size_t size) {
    void *p = malloc(size);
    if (unlikely(!p)) { fprintf(stderr, "decode: malloc(%zu) failed\n", size); abort(); }
    return p;
}
static inline void *_decode_calloc(size_t size) {
    void *p = _decode_malloc(size);
    memset(p, 0, size);
    return p;
}

#define cmalloc(size)   _decode_malloc(size)
#define cmCalloc(size)  _decode_calloc(size)
#define sfree(x)        do { free(x); (x) = NULL; } while (0)

/* ── fasthash inline (from fasthash.h / aircraft.h) ────────────────────── */

#define mix_fasthash(h) ({              \
    (h) ^= (h) >> 23;                   \
    (h) *= 0x2127599bf4325c37ULL;       \
    (h) ^= (h) >> 47; })

static inline uint32_t addrHash(uint32_t addr, uint32_t bits) {
    const uint64_t m    = 0x880355f21e6d1965ULL;
    const uint64_t seed = 0x30732349f7810465ULL;
    uint64_t h = seed ^ (4 * m);
    uint64_t v = addr;
    h ^= mix_fasthash(v);
    h *= m;
    return (uint32_t)(h >> (64 - bits));
}

/* ── Protocol constants ─────────────────────────────────────────────────── */

#define MODES_SHORT_MSG_BITS   56
#define MODES_SHORT_MSG_BYTES  (MODES_SHORT_MSG_BITS / 8)   /* 7  */
#define MODES_LONG_MSG_BITS    112
#define MODES_LONG_MSG_BYTES   (MODES_LONG_MSG_BITS  / 8)   /* 14 */

#define MODES_SHORT_MSG_SAMPLES (MODES_SHORT_MSG_BITS * 2)
#define MODES_LONG_MSG_SAMPLES  (MODES_LONG_MSG_BITS  * 2)

#define MODES_MAX_BITERRORS    2

#define HEX_UNKNOWN            0xDEADBEEFU
#define DFTYPE_MODEAC          77
#define INVALID_ALTITUDE       (-9999)
#define MODES_NON_ICAO_ADDRESS (1u << 24)

#define MAGIC_MLAT_TIMESTAMP      0xFF004D4C4154LL
#define MAGIC_NOFORWARD_TIMESTAMP 0xFF004D4C4160LL
#define MAGIC_UAT_TIMESTAMP       0xFF004D4C4155LL

/* Time unit constants (milliseconds) */
#define SECONDS (1000LL)
#define MINUTES (60*1000LL)
#define HOURS   (3600*1000LL)

/* ── Squawk / Mode-A helpers (from readsb/track.h) ─────────────────────── */

static inline uint32_t squawkHex2Dec(uint32_t s) {
    return ((s & 0xf000) / 0x1000 * 1000 + (s & 0x0f00) / 0x100 * 100
          + (s & 0x00f0) / 0x10  *   10 + (s & 0x000f));
}

/* Convert (hex) Mode-A squawk to a 0-4095 table index */
static inline unsigned modeAToIndex(unsigned modeA) {
    return (modeA & 0x0007) | ((modeA & 0x0070) >> 1)
         | ((modeA & 0x0700) >> 2) | ((modeA & 0x7000) >> 3);
}

/* Convert 0-4095 index back to (hex) Mode-A squawk */
static inline unsigned indexToModeA(unsigned index) {
    return (index & 007) | ((index & 070) << 1)
         | ((index & 0700) << 2) | ((index & 07000) << 3);
}

/* Converts Mode-A squawk to altitude (feet); defined in mode_ac.c */
int modeAToModeC(unsigned int modeA);

/* Stub declarations for displayModesMessage() — we never call it */
static inline const char *addrtype_enum_string(int t) { (void)t; return "?"; }
static inline const char *airground_to_string(int a)  { (void)a; return "?"; }
static inline const char *cpr_type_string(int c)      { (void)c; return "?"; }
static inline const char *addrtype_to_string(int t)   { (void)t; return "?"; }
static inline void printACASInfoShort(uint32_t a, unsigned char *mv,
                                      void *cl, void *mm, int64_t ts) {
    (void)a; (void)mv; (void)cl; (void)mm; (void)ts;
}
static inline void sprint_uuid1(uint64_t id, char *buf) { (void)id; buf[0] = '?'; buf[1] = 0; }

/* ── Enumerated types (from readsb.h lines 163-298) ────────────────────── */

typedef enum {
    SOURCE_INVALID,
    SOURCE_INDIRECT,
    SOURCE_MODE_AC,
    SOURCE_SBS,
    SOURCE_MLAT,
    SOURCE_MODE_S,
    SOURCE_JAERO,
    SOURCE_MODE_S_CHECKED,
    SOURCE_TISB,
    SOURCE_ADSR,
    SOURCE_ADSB,
    SOURCE_PRIO,
} datasource_t;

typedef enum {
    ADDR_ADSB_ICAO      = 0,
    ADDR_ADSB_ICAO_NT   = 1,
    ADDR_ADSR_ICAO      = 2,
    ADDR_TISB_ICAO      = 3,
    ADDR_JAERO          = 4,
    ADDR_MLAT           = 5,
    ADDR_OTHER          = 6,
    ADDR_MODE_S         = 7,
    ADDR_ADSB_OTHER     = 8,
    ADDR_ADSR_OTHER     = 9,
    ADDR_TISB_TRACKFILE = 10,
    ADDR_TISB_OTHER     = 11,
    ADDR_MODE_A         = 12,
    ADDR_UNKNOWN        = 13,
} addrtype_t;

#define NUM_TYPES 14

typedef enum { UNIT_FEET, UNIT_METERS } altitude_unit_t;
typedef enum { ALTITUDE_BARO, ALTITUDE_GEOM } altitude_source_t;

typedef enum {
    AG_INVALID  = 0,
    AG_GROUND   = 1,
    AG_AIRBORNE = 2,
    AG_UNCERTAIN = 3,
} airground_t;

typedef enum { SIL_INVALID, SIL_UNKNOWN, SIL_PER_SAMPLE, SIL_PER_HOUR } sil_type_t;
typedef enum { CPR_INVALID, CPR_SURFACE, CPR_AIRBORNE, CPR_COARSE }      cpr_type_t;
typedef enum { CPR_NONE, CPR_LOCAL, CPR_GLOBAL }                          cpr_local_t;

typedef enum {
    HEADING_INVALID,
    HEADING_GROUND_TRACK,
    HEADING_TRUE,
    HEADING_MAGNETIC,
    HEADING_MAGNETIC_OR_TRUE,
    HEADING_TRACK_OR_HEADING,
} heading_type_t;

typedef enum {
    COMMB_UNKNOWN,
    COMMB_AMBIGUOUS,
    COMMB_EMPTY_RESPONSE,
    COMMB_DATALINK_CAPS,
    COMMB_GICB_CAPS,
    COMMB_AIRCRAFT_IDENT,
    COMMB_ACAS_RA,
    COMMB_VERTICAL_INTENT,
    COMMB_TRACK_TURN,
    COMMB_HEADING_SPEED,
    COMMB_METEOROLOGICAL_ROUTINE,
} commb_format_t;

typedef enum {
    NAV_MODE_AUTOPILOT = 1,
    NAV_MODE_VNAV      = 2,
    NAV_MODE_ALT_HOLD  = 4,
    NAV_MODE_APPROACH  = 8,
    NAV_MODE_LNAV      = 16,
    NAV_MODE_TCAS      = 32,
} nav_modes_t;

typedef enum {
    EMERGENCY_NONE     = 0,
    EMERGENCY_GENERAL  = 1,
    EMERGENCY_LIFEGUARD = 2,
    EMERGENCY_MINFUEL  = 3,
    EMERGENCY_NORDO    = 4,
    EMERGENCY_UNLAWFUL = 5,
    EMERGENCY_DOWNED   = 6,
    EMERGENCY_RESERVED = 7,
} emergency_t;

typedef enum {
    NAV_ALT_INVALID,
    NAV_ALT_UNKNOWN,
    NAV_ALT_AIRCRAFT,
    NAV_ALT_MCP,
    NAV_ALT_FMS,
} nav_altitude_source_t;

/* ── Forward declarations for pointer fields in modesMessage ────────────── */

struct client;
struct aircraft;
struct messageBuffer;
struct modeMessage;

/* ── Full struct modesMessage (from readsb.h lines 981-1238) ────────────── */

struct modesMessage {
    unsigned char msg[MODES_LONG_MSG_BYTES];
    unsigned char verbatim[MODES_LONG_MSG_BYTES];
    double signalLevel;
    struct client      *client;
    struct aircraft    *aircraft;
    struct messageBuffer *messageBuffer;

    int64_t  timestamp;
    int64_t  sysTimestamp;
    uint64_t receiverId;
    int      msgtype;
    int      msgbits;
    int      score;
    int      receiverCountMlat;
    int      mlatEPU;
    int      correctedbits;
    int      decodeResult;
    uint32_t crc;
    uint32_t addr;
    uint32_t maybe_addr;
    addrtype_t addrtype;
    int8_t remote;
    int8_t sbs_in;
    int8_t address_reliable;
    int8_t sbsMsgType;
    int8_t reduce_forward;
    int8_t garbage;
    int8_t duplicate;
    int8_t duplicate_checked;
    int8_t pos_bad;
    int8_t pos_ignore;
    int8_t pos_old;
    int8_t pos_receiver_range_exceeded;
    int8_t trackUnreliable;
    int8_t speedUnreliable;
    int8_t in_disc_cache;
    int8_t jsonPositionOutputEmit;
    datasource_t source;

    unsigned IID, AA, AC, CA, CC, CF, DR, FS, ID, KE, ND, RI, SL, UM, VS;
    unsigned metype;
    unsigned mesub;

    unsigned char MB[7];
    unsigned char MD[10];
    unsigned char ME[7];
    unsigned char MV[7];

    bool baro_alt_valid;
    bool geom_alt_valid;
    bool track_valid;
    bool track_rate_valid;
    bool heading_valid;
    bool roll_valid;
    bool gs_valid;
    bool ias_valid;
    bool tas_valid;
    bool mach_valid;
    bool baro_rate_valid;
    bool geom_rate_valid;
    bool squawk_valid;
    bool callsign_valid;
    bool cpr_valid;
    bool cpr_odd;
    bool cpr_decoded;
    bool cpr_relative;
    bool category_valid;
    bool geom_delta_valid;
    bool from_mlat;
    bool from_tisb;
    bool spi_valid;
    bool spi;
    bool alert_valid;
    bool alert;
    bool emergency_valid;
    bool sbs_pos_valid;
    bool alt_q_bit;
    bool acas_ra_valid;
    bool geom_alt_derived;
    bool wind_valid;
    bool oat_valid;
    bool static_pressure_valid;
    bool turbulence_valid;
    bool humidity_valid;
    bool met_source_valid;
    bool squawk_emergency_valid;
    bool squawk_emergency;

    int baro_alt;
    altitude_unit_t baro_alt_unit;
    int geom_alt;
    altitude_unit_t geom_alt_unit;

    int geom_delta;
    float heading;
    heading_type_t heading_type;
    float track_rate;
    float roll;

    struct { float v0, v2, selected; } gs;
    unsigned ias;
    unsigned tas;
    double mach;
    int baro_rate;
    int geom_rate;
    char callsign[16];
    uint32_t squawkHex;
    uint32_t squawkDec;
    unsigned category;
    emergency_t emergency;

    cpr_type_t cpr_type;
    uint32_t cpr_lat;
    uint32_t cpr_lon;
    uint32_t cpr_nucp;

    airground_t airground;

    double decoded_lat;
    double decoded_lon;
    unsigned decoded_nic;
    unsigned decoded_rc;

    double distance_traveled;
    double receiver_distance;
    float calculated_track;

    int wind_speed;
    float wind_direction;
    float oat;
    int static_pressure;
    int turbulence;
    float humidity;
    int met_source;

    commb_format_t commb_format;

    struct {
        bool nic_a_valid, nic_b_valid, nic_c_valid, nic_baro_valid;
        bool nac_p_valid, nac_v_valid, gva_valid, sda_valid;
        bool nic_a, nic_b, nic_c, nic_baro;
        unsigned nac_p, nac_v, sil, gva, sda;
        sil_type_t sil_type;
    } accuracy;

    struct {
        sil_type_t sil_type;
        heading_type_t tah;
        heading_type_t hrd;
        enum { ANGLE_HEADING, ANGLE_TRACK } track_angle;
        unsigned cc_lw;
        unsigned cc_antenna_offset;
        unsigned valid : 1;
        unsigned version : 3;
        unsigned om_acas_ra : 1;
        unsigned om_ident : 1;
        unsigned om_atc : 1;
        unsigned om_saf : 1;
        unsigned cc_acas : 1;
        unsigned cc_cdti : 1;
        unsigned cc_1090_in : 1;
        unsigned cc_arv : 1;
        unsigned cc_ts : 1;
        unsigned cc_tc : 2;
        unsigned cc_uat_in : 1;
        unsigned cc_poa : 1;
        unsigned cc_b2_low : 1;
        unsigned cc_lw_valid : 1;
    } opstatus;

    struct {
        unsigned fms_altitude;
        unsigned mcp_altitude;
        float qnh;
        float heading;
        bool heading_valid;
        bool fms_altitude_valid;
        bool mcp_altitude_valid;
        bool qnh_valid;
        bool modes_valid;
        heading_type_t heading_type;
        nav_altitude_source_t altitude_source;
        nav_modes_t modes;
    } nav;
};

/* ── Minimal Modes global stub (12 refs in mode_s.c, 1 in comm_b.c) ─────── */

struct _decode_modes_t {
    int fixDF;          /* 1 = try to fix corrupted DF field */
    int nfix_crc;       /* max CRC bits to correct (1) */
    int decode_all;     /* 0 = only decode known DFs */
    int net_verbatim;   /* 0 = don't forward verbatim */
    int netIngest;      /* 0 = not a network ingest node */
    int garbage_ports;  /* 0 = no garbage output ports */
    int debug_callsign; /* 0 = no callsign debug output */
    int debug_bogus;    /* 0 = no bogus message debug output */
    int onlyaddr;       /* 0 = full decode, not addr-only */
    int mlat;           /* 0 = mlat output disabled */
    int raw;            /* 0 = not raw-only mode */
    struct { int cpr_filtered; } stats_current;
};

extern struct _decode_modes_t Modes;

/* Pull in sub-module APIs — must come after struct modesMessage is defined */
#include "crc.h"
#include "icao_filter.h"
#include "comm_b.h"
#include "mode_s.h"
