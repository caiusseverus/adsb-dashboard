#include "decode_api.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    double arrival_us;
    double signal_dbfs;
    bool has_signal;
} radar_reply_t;

typedef struct {
    double values[30];
    int count;
} radar_centroid_history_t;

typedef struct {
    uint32_t icao;
    double last_arrival_us;
    radar_reply_t *replies;
    int reply_count;
    int reply_capacity;
    radar_centroid_history_t centroid_history;
} radar_pending_burst_t;

struct radar_burst_processor_t {
    radar_pending_burst_t *pending;
    int pending_count;
    int pending_capacity;
};

static radar_pending_burst_t *find_pending(
    radar_burst_processor_t *processor,
    uint32_t icao,
    bool create
) {
    int i;
    for (i = 0; i < processor->pending_count; i++) {
        if (processor->pending[i].icao == icao) {
            return &processor->pending[i];
        }
    }
    if (!create) {
        return NULL;
    }
    if (processor->pending_count >= processor->pending_capacity) {
        int new_capacity = processor->pending_capacity > 0 ? processor->pending_capacity * 2 : 8;
        radar_pending_burst_t *new_pending = realloc(
            processor->pending,
            (size_t)new_capacity * sizeof(radar_pending_burst_t)
        );
        if (!new_pending) {
            return NULL;
        }
        processor->pending = new_pending;
        processor->pending_capacity = new_capacity;
    }
    memset(&processor->pending[processor->pending_count], 0, sizeof(radar_pending_burst_t));
    processor->pending[processor->pending_count].icao = icao;
    processor->pending_count += 1;
    return &processor->pending[processor->pending_count - 1];
}

static bool append_reply(radar_pending_burst_t *pending, const radar_burst_event_t *event) {
    if (pending->reply_count >= pending->reply_capacity) {
        int new_capacity = pending->reply_capacity > 0 ? pending->reply_capacity * 2 : 4;
        radar_reply_t *new_replies = realloc(
            pending->replies,
            (size_t)new_capacity * sizeof(radar_reply_t)
        );
        if (!new_replies) {
            return false;
        }
        pending->replies = new_replies;
        pending->reply_capacity = new_capacity;
    }
    pending->replies[pending->reply_count].arrival_us = event->arrival_us;
    pending->replies[pending->reply_count].signal_dbfs = event->signal_dbfs;
    pending->replies[pending->reply_count].has_signal = event->has_signal;
    pending->reply_count += 1;
    pending->last_arrival_us = event->arrival_us;
    return true;
}

static radar_fired_burst_t finalize_pending_burst(
    radar_pending_burst_t *pending,
    double trigger_arrival_us
) {
    int i;
    double total_w = 0.0;
    double weighted_sum = 0.0;
    double burst_signal = 0.0;
    bool has_signal = false;
    radar_fired_burst_t burst;

    memset(&burst, 0, sizeof(burst));
    burst.icao = pending->icao;
    burst.trigger_arrival_us = trigger_arrival_us;

    for (i = 0; i < pending->reply_count; i++) {
        double w = 1.0;
        if (pending->replies[i].has_signal) {
            w = pow(10.0, pending->replies[i].signal_dbfs / 20.0);
            if (!has_signal || pending->replies[i].signal_dbfs > burst_signal) {
                burst_signal = pending->replies[i].signal_dbfs;
                has_signal = true;
            }
        }
        weighted_sum += pending->replies[i].arrival_us * w;
        total_w += w;
    }

    if (total_w > 0.0) {
        burst.burst_centroid_us = weighted_sum / total_w;
    } else if (pending->reply_count > 0) {
        burst.burst_centroid_us = pending->replies[pending->reply_count - 1].arrival_us;
    }
    if (pending->centroid_history.count < 30) {
        pending->centroid_history.values[pending->centroid_history.count++] = burst.burst_centroid_us;
    } else {
        memmove(
            pending->centroid_history.values,
            pending->centroid_history.values + 1,
            (size_t)29 * sizeof(double)
        );
        pending->centroid_history.values[29] = burst.burst_centroid_us;
    }
    burst.has_signal = has_signal;
    burst.burst_signal_dbfs = burst_signal;
    burst.n_replies = pending->reply_count;
    return burst;
}

static int compare_burst_centroid(const void *lhs, const void *rhs) {
    const radar_fired_burst_t *left = lhs;
    const radar_fired_burst_t *right = rhs;
    if (left->burst_centroid_us < right->burst_centroid_us) {
        return -1;
    }
    if (left->burst_centroid_us > right->burst_centroid_us) {
        return 1;
    }
    return 0;
}

static int compare_double(const void *lhs, const void *rhs) {
    double left = *(const double *)lhs;
    double right = *(const double *)rhs;
    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

static radar_pending_burst_t *find_history(radar_burst_processor_t *processor, uint32_t icao) {
    return find_pending(processor, icao, false);
}

static int collect_valid_intervals(
    const radar_centroid_history_t *history,
    double *out_intervals,
    int max_out
) {
    int i;
    int out_count = 0;
    if (!history || history->count < 2) {
        return 0;
    }
    for (i = 0; i < history->count - 1 && out_count < max_out; i++) {
        double interval_s = (history->values[i + 1] - history->values[i]) / 1000000.0;
        if (interval_s > 0.5 && interval_s < 30.0) {
            out_intervals[out_count++] = interval_s;
        }
    }
    return out_count;
}

static double median_of_sorted(const double *values, int count) {
    if (count <= 0) {
        return 0.0;
    }
    if ((count & 1) == 1) {
        return values[count / 2];
    }
    return (values[(count / 2) - 1] + values[count / 2]) / 2.0;
}

static double mean_of_values(const double *values, int count) {
    int i;
    double sum = 0.0;
    if (count <= 0) {
        return 0.0;
    }
    for (i = 0; i < count; i++) {
        sum += values[i];
    }
    return sum / (double)count;
}

static double compute_stddev(const double *values, int count, double mean) {
    int i;
    double acc = 0.0;
    if (count <= 1) {
        return 0.0;
    }
    for (i = 0; i < count; i++) {
        double delta = values[i] - mean;
        acc += delta * delta;
    }
    return sqrt(acc / (double)(count - 1));
}

radar_burst_processor_t *radar_burst_processor_create(void) {
    return calloc(1, sizeof(radar_burst_processor_t));
}

void radar_burst_processor_destroy(radar_burst_processor_t *processor) {
    int i;
    if (!processor) {
        return;
    }
    for (i = 0; i < processor->pending_count; i++) {
        free(processor->pending[i].replies);
    }
    free(processor->pending);
    free(processor);
}

int radar_burst_processor_process(
    radar_burst_processor_t *processor,
    const radar_burst_event_t *events,
    int event_count,
    double burst_gap_us,
    radar_fired_burst_t *out_bursts,
    int max_out_bursts
) {
    int event_index;
    int out_count = 0;

    if (!processor || !events || event_count <= 0 || !out_bursts || max_out_bursts <= 0) {
        return 0;
    }

    for (event_index = 0; event_index < event_count; event_index++) {
        radar_fired_burst_t *expired = NULL;
        int expired_count = 0;
        int expired_capacity = processor->pending_count > 0 ? processor->pending_count : 1;
        int pending_index = 0;
        const radar_burst_event_t *event = &events[event_index];
        expired = malloc((size_t)expired_capacity * sizeof(radar_fired_burst_t));
        if (!expired) {
            return out_count;
        }

        while (pending_index < processor->pending_count) {
            radar_pending_burst_t *pending = &processor->pending[pending_index];
            if ((event->arrival_us - pending->last_arrival_us) > burst_gap_us) {
                if (expired_count < expired_capacity && pending->reply_count > 0) {
                    expired[expired_count++] = finalize_pending_burst(pending, event->arrival_us);
                }
                pending->reply_count = 0;
                pending->last_arrival_us = event->arrival_us;
            }
            pending_index += 1;
        }

        if (expired_count > 1) {
            qsort(expired, (size_t)expired_count, sizeof(expired[0]), compare_burst_centroid);
        }
        if (expired_count > 0) {
            int copy_count = expired_count;
            if (copy_count > max_out_bursts - out_count) {
                copy_count = max_out_bursts - out_count;
            }
            if (copy_count > 0) {
                memcpy(&out_bursts[out_count], expired, (size_t)copy_count * sizeof(expired[0]));
                out_count += copy_count;
                if (out_count >= max_out_bursts) {
                    free(expired);
                    return out_count;
                }
            }
        }
        free(expired);

        radar_pending_burst_t *pending = find_pending(processor, event->icao, true);
        if (!pending) {
            return out_count;
        }
        if (!append_reply(pending, event)) {
            return out_count;
        }
    }

    return out_count;
}

int radar_burst_processor_matches_dominant_period(
    radar_burst_processor_t *processor,
    uint32_t icao,
    double period_s,
    int min_bursts,
    double tolerance
) {
    radar_pending_burst_t *pending = find_history(processor, icao);
    double intervals[29];
    int count;
    double median_period;
    if (!pending || pending->centroid_history.count < min_bursts || period_s <= 0.0) {
        return 0;
    }
    count = collect_valid_intervals(&pending->centroid_history, intervals, 29);
    if (count < 2) {
        return 0;
    }
    qsort(intervals, (size_t)count, sizeof(double), compare_double);
    median_period = median_of_sorted(intervals, count);
    return fabs(median_period - period_s) / period_s <= tolerance ? 1 : 0;
}

int radar_burst_processor_matches_phase_family(
    radar_burst_processor_t *processor,
    uint32_t ref_icao,
    double ref_arrival_us,
    uint32_t icao,
    double burst_centroid_us,
    double period_s,
    int min_history,
    double tolerance_us
) {
    radar_pending_burst_t *ref_pending = find_history(processor, ref_icao);
    radar_pending_burst_t *obs_pending = find_history(processor, icao);
    double period_us;
    double offsets[30];
    int offset_count = 0;
    int i;
    if (icao == ref_icao || period_s <= 0.0) {
        return 1;
    }
    if (!ref_pending || !obs_pending) {
        return 1;
    }
    if (ref_pending->centroid_history.count <= 0 || obs_pending->centroid_history.count <= 0) {
        return 1;
    }
    period_us = period_s * 1000000.0;
    for (i = 0; i < obs_pending->centroid_history.count && offset_count < 30; i++) {
        double obs_centroid = obs_pending->centroid_history.values[i];
        double nearest_delta = -1.0;
        int j;
        if (obs_centroid >= burst_centroid_us) {
            continue;
        }
        for (j = 0; j < ref_pending->centroid_history.count; j++) {
            double ref_centroid = ref_pending->centroid_history.values[j];
            double delta;
            if (ref_centroid >= ref_arrival_us) {
                continue;
            }
            delta = fabs(obs_centroid - ref_centroid);
            if (nearest_delta < 0.0 || delta < nearest_delta) {
                nearest_delta = delta;
                offsets[offset_count] = fmod(obs_centroid - ref_centroid + period_us, period_us);
            }
        }
        if (nearest_delta >= 0.0) {
            offset_count += 1;
        }
    }
    if (offset_count < min_history) {
        return 1;
    }
    qsort(offsets, (size_t)offset_count, sizeof(double), compare_double);
    {
        double expected_offset_us = median_of_sorted(offsets, offset_count);
        double observed_offset_us = fmod(burst_centroid_us - ref_arrival_us + period_us, period_us);
        double delta = fabs(observed_offset_us - expected_offset_us);
        double circular_delta = delta < (period_us - delta) ? delta : (period_us - delta);
        return circular_delta <= tolerance_us ? 1 : 0;
    }
}

uint32_t radar_burst_processor_select_reference(
    radar_burst_processor_t *processor,
    double period_s,
    double now_us,
    int min_bursts_for_ref,
    double recency_periods,
    double hysteresis,
    uint32_t current_ref_icao
) {
    int i;
    uint32_t best_icao = 0;
    double best_score = 0.0;
    double current_score = -1.0;
    double recency_threshold_us;
    if (!processor || period_s <= 0.0) {
        return 0;
    }
    recency_threshold_us = period_s * recency_periods * 1000000.0;
    for (i = 0; i < processor->pending_count; i++) {
        radar_pending_burst_t *pending = &processor->pending[i];
        double intervals[29];
        int count;
        double median_period;
        double std_dev;
        double period_dev;
        double count_factor;
        double score;
        if (pending->centroid_history.count < min_bursts_for_ref) {
            continue;
        }
        if (now_us > 0.0) {
            double last_centroid = pending->centroid_history.values[pending->centroid_history.count - 1];
            if ((now_us - last_centroid) > recency_threshold_us) {
                continue;
            }
        }
        count = collect_valid_intervals(&pending->centroid_history, intervals, 29);
        if (count < 2) {
            continue;
        }
        qsort(intervals, (size_t)count, sizeof(double), compare_double);
        median_period = median_of_sorted(intervals, count);
        std_dev = compute_stddev(intervals, count, mean_of_values(intervals, count));
        period_dev = fabs(median_period - period_s) / period_s;
        count_factor = 1.0 / (double)pending->centroid_history.count;
        score = period_dev + (std_dev / period_s) + count_factor;
        if (best_icao == 0 || score < best_score) {
            best_icao = pending->icao;
            best_score = score;
        }
        if (pending->icao == current_ref_icao) {
            current_score = score;
        }
    }
    if (best_icao == 0) {
        return 0;
    }
    if (current_ref_icao != 0 && current_score >= 0.0) {
        if (best_score > current_score * (1.0 - hysteresis)) {
            return current_ref_icao;
        }
    }
    return best_icao;
}
