-- stats_extension--1.0.sql
--
-- This extension packages a collection of statistical aggregate functions that
-- extend PostgreSQL's analytics capabilities. The functions provided here
-- include robust measures such as the median, mode, trimmed mean, median
-- absolute deviation (MAD), quartiles, inter‑quartile range (IQR), range,
-- coefficient of variation, skewness, kurtosis, weighted mean and the
-- Gini coefficient.  By bundling these as an extension you can install
-- them once using CREATE EXTENSION and then use them like any other
-- built‑in aggregate on your tables.  All aggregates operate on columns of
-- numeric values and are designed to ignore NULL input values.  Where a
-- trimmed mean fraction is not provided the default cut‑off is 10%.

-- Create a composite type to hold state for the trimmed mean aggregate.
CREATE TYPE stats_extension_trim_state AS (
    values   numeric[],
    fraction numeric
);

-- State transition function for the trimmed mean.  It appends the current
-- value to the array of values and stores the trim fraction.  If a NULL
-- fraction is supplied no update is made and the default will be used
-- during the final calculation.
CREATE OR REPLACE FUNCTION stats_extension.trimmed_mean_state(
    state stats_extension_trim_state,
    val   numeric,
    frac  numeric
) RETURNS stats_extension_trim_state AS $$
BEGIN
    IF state.values IS NULL THEN
        state.values := ARRAY[]::numeric[];
    END IF;
    -- append the incoming value to the values array
    state.values := array_append(state.values, val);
    -- remember the trim fraction if supplied (assumes constant across rows)
    IF frac IS NOT NULL THEN
        state.fraction := frac;
    END IF;
    RETURN state;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Final function for trimmed mean.  Sorts the collected values and
-- discards a fraction of observations from both ends before averaging.  If
-- no fraction was supplied the default of 0.1 (10%) is used.
CREATE OR REPLACE FUNCTION stats_extension.trimmed_mean_final(
    state stats_extension_trim_state
) RETURNS numeric AS $$
DECLARE
    sorted     numeric[];
    n          integer;
    start_idx  integer;
    end_idx    integer;
    sum_val    numeric := 0;
    count_val  integer := 0;
    i          integer;
BEGIN
    -- No data
    IF state.values IS NULL OR array_length(state.values,1) IS NULL OR array_length(state.values,1) = 0 THEN
        RETURN NULL;
    END IF;
    -- Use default fraction when none is provided
    IF state.fraction IS NULL THEN
        state.fraction := 0.1;
    END IF;
    -- Sort the values in ascending order
    SELECT array_agg(v ORDER BY v) INTO sorted
    FROM unnest(state.values) AS v;
    n := array_length(sorted,1);
    -- Determine start and end indices after trimming
    start_idx := ceil(state.fraction * n + 1);
    end_idx   := n - floor(state.fraction * n);
    -- If trimming removes all data return NULL
    IF start_idx > end_idx THEN
        RETURN NULL;
    END IF;
    FOR i IN start_idx..end_idx LOOP
        sum_val   := sum_val + sorted[i];
        count_val := count_val + 1;
    END LOOP;
    RETURN sum_val / count_val;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Median final function.  Uses simple array sorting to pick the middle
-- element(s) and compute their mean when the sample size is even.
CREATE OR REPLACE FUNCTION stats_extension._final_median(
    arr numeric[]
) RETURNS numeric AS $$
SELECT AVG(val)
FROM (
    SELECT val
    FROM unnest(arr) AS val
    ORDER BY val
    LIMIT 2 - MOD(array_length(arr,1), 2)
    OFFSET CEIL(array_length(arr,1) / 2.0) - 1
) sub;
$$ LANGUAGE sql IMMUTABLE;

-- Aggregate to compute the median of numeric values
CREATE AGGREGATE stats_extension.median(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_median,
    INITCOND = '{}'
);

-- State and final functions for the geometric mean.  The state holds
-- accumulated sums of logarithms and the count of values.  The final
-- function returns the exponent of the average log.
CREATE OR REPLACE FUNCTION stats_extension.geom_mean_state(
    state numeric[],
    val   numeric
) RETURNS numeric[] AS $$
BEGIN
    IF state IS NULL OR array_length(state,1) <> 2 THEN
        state := ARRAY[0::numeric, 0::numeric];
    END IF;
    IF val <= 0 THEN
        RAISE EXCEPTION 'Geometric mean is undefined for non‑positive values';
    END IF;
    state[1] := state[1] + ln(val);
    state[2] := state[2] + 1;
    RETURN state;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION stats_extension.geom_mean_final(
    state numeric[]
) RETURNS numeric AS $$
BEGIN
    IF state IS NULL OR state[2] = 0 THEN
        RETURN NULL;
    END IF;
    RETURN exp(state[1] / state[2]);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.geometric_mean(numeric) (
    SFUNC    = stats_extension.geom_mean_state,
    STYPE    = numeric[],
    FINALFUNC = stats_extension.geom_mean_final,
    INITCOND = '{0,0}'
);

-- State and final functions for the harmonic mean.  The state stores the
-- sum of reciprocals and the count of values.  The final returns
-- count/sum(reciprocal).
CREATE OR REPLACE FUNCTION stats_extension.harm_mean_state(
    state numeric[],
    val   numeric
) RETURNS numeric[] AS $$
BEGIN
    IF state IS NULL OR array_length(state,1) <> 2 THEN
        state := ARRAY[0::numeric, 0::numeric];
    END IF;
    IF val = 0 THEN
        RAISE EXCEPTION 'Harmonic mean is undefined for zero values';
    END IF;
    state[1] := state[1] + (1/val);
    state[2] := state[2] + 1;
    RETURN state;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION stats_extension.harm_mean_final(
    state numeric[]
) RETURNS numeric AS $$
BEGIN
    IF state IS NULL OR state[2] = 0 THEN
        RETURN NULL;
    END IF;
    RETURN state[2] / state[1];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.harmonic_mean(numeric) (
    SFUNC    = stats_extension.harm_mean_state,
    STYPE    = numeric[],
    FINALFUNC = stats_extension.harm_mean_final,
    INITCOND = '{0,0}'
);

-- Mode final function.  Selects the value with the highest frequency (ties
-- are broken by choosing the smallest value).
CREATE OR REPLACE FUNCTION stats_extension._final_mode(
    arr numeric[]
) RETURNS numeric AS $$
SELECT val
FROM (
    SELECT val, COUNT(*) AS freq
    FROM unnest(arr) AS val
    GROUP BY val
    ORDER BY freq DESC, val
    LIMIT 1
) sub;
$$ LANGUAGE sql IMMUTABLE;

CREATE AGGREGATE stats_extension.mode(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_mode,
    INITCOND = '{}'
);

-- Final function for median absolute deviation (MAD).  Computes the
-- median of absolute deviations from the median.
CREATE OR REPLACE FUNCTION stats_extension._final_mad(
    arr numeric[]
) RETURNS numeric AS $$
DECLARE
    med    numeric;
    abs_arr numeric[];
BEGIN
    med := stats_extension._final_median(arr);
    SELECT array_agg(abs(val - med)) INTO abs_arr
    FROM unnest(arr) AS val;
    RETURN stats_extension._final_median(abs_arr);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.mad(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_mad,
    INITCOND = '{}'
);

-- Quartile aggregates using percentile_cont.  These aggregates return the
-- 25th and 75th percentiles respectively.
CREATE OR REPLACE FUNCTION stats_extension._final_quartile1(
    arr numeric[]
) RETURNS numeric AS $$
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY val)
FROM unnest(arr) AS val;
$$ LANGUAGE sql IMMUTABLE;

CREATE AGGREGATE stats_extension.quartile1(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_quartile1,
    INITCOND = '{}'
);

CREATE OR REPLACE FUNCTION stats_extension._final_quartile3(
    arr numeric[]
) RETURNS numeric AS $$
SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY val)
FROM unnest(arr) AS val;
$$ LANGUAGE sql IMMUTABLE;

CREATE AGGREGATE stats_extension.quartile3(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_quartile3,
    INITCOND = '{}'
);

-- Interquartile range is the difference between Q3 and Q1
CREATE OR REPLACE FUNCTION stats_extension._final_iqr(
    arr numeric[]
) RETURNS numeric AS $$
DECLARE
    q1 numeric;
    q3 numeric;
BEGIN
    q1 := stats_extension._final_quartile1(arr);
    q3 := stats_extension._final_quartile3(arr);
    RETURN q3 - q1;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.interquartile_range(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_iqr,
    INITCOND = '{}'
);

-- Range aggregate (max minus min)
CREATE OR REPLACE FUNCTION stats_extension._final_range(
    arr numeric[]
) RETURNS numeric AS $$
SELECT MAX(val) - MIN(val)
FROM unnest(arr) AS val;
$$ LANGUAGE sql IMMUTABLE;

CREATE AGGREGATE stats_extension.range_agg(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_range,
    INITCOND = '{}'
);

-- Coefficient of variation (standard deviation divided by mean)
CREATE OR REPLACE FUNCTION stats_extension._final_cv(
    arr numeric[]
) RETURNS numeric AS $$
DECLARE
    m  numeric;
    sd numeric;
BEGIN
    SELECT AVG(val), STDDEV_POP(val) INTO m, sd
    FROM unnest(arr) AS val;
    IF m = 0 THEN
        RETURN NULL;
    END IF;
    RETURN sd / m;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.coefficient_of_variation(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_cv,
    INITCOND = '{}'
);

-- Skewness aggregate.  Calculates the population skewness (third moment).
CREATE OR REPLACE FUNCTION stats_extension._final_skewness(
    arr numeric[]
) RETURNS numeric AS $$
DECLARE
    n        numeric;
    m        numeric;
    sd       numeric;
    sum_cubed numeric;
BEGIN
    SELECT COUNT(val), AVG(val), STDDEV_POP(val) INTO n, m, sd
    FROM unnest(arr) AS val;
    IF n = 0 OR sd = 0 THEN
        RETURN NULL;
    END IF;
    SELECT SUM(POWER((val - m)/sd, 3)) INTO sum_cubed
    FROM unnest(arr) AS val;
    RETURN sum_cubed / n;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.skewness(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_skewness,
    INITCOND = '{}'
);

-- Kurtosis aggregate.  Returns excess kurtosis (fourth moment minus 3).
CREATE OR REPLACE FUNCTION stats_extension._final_kurtosis(
    arr numeric[]
) RETURNS numeric AS $$
DECLARE
    n        numeric;
    m        numeric;
    sd       numeric;
    sum_quad numeric;
BEGIN
    SELECT COUNT(val), AVG(val), STDDEV_POP(val) INTO n, m, sd
    FROM unnest(arr) AS val;
    IF n = 0 OR sd = 0 THEN
        RETURN NULL;
    END IF;
    SELECT SUM(POWER((val - m)/sd, 4)) INTO sum_quad
    FROM unnest(arr) AS val;
    -- subtract 3 so that a normal distribution has kurtosis 0
    RETURN sum_quad / n - 3;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.kurtosis(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_kurtosis,
    INITCOND = '{}'
);

-- Weighted mean.  Uses a two‑element state array to accumulate the sum of
-- weight*value and the sum of weights.  The final function divides these.
CREATE OR REPLACE FUNCTION stats_extension.weighted_mean_state(
    state numeric[],
    val   numeric,
    weight numeric
) RETURNS numeric[] AS $$
BEGIN
    IF state IS NULL OR array_length(state,1) <> 2 THEN
        state := ARRAY[0::numeric, 0::numeric];
    END IF;
    -- ignore rows with NULL value or weight
    IF weight IS NULL OR val IS NULL THEN
        RETURN state;
    END IF;
    state[1] := state[1] + (val * weight);
    state[2] := state[2] + weight;
    RETURN state;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION stats_extension.weighted_mean_final(
    state numeric[]
) RETURNS numeric AS $$
BEGIN
    IF state IS NULL OR state[2] = 0 THEN
        RETURN NULL;
    END IF;
    RETURN state[1] / state[2];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.weighted_mean(numeric, numeric) (
    SFUNC    = stats_extension.weighted_mean_state,
    STYPE    = numeric[],
    FINALFUNC = stats_extension.weighted_mean_final,
    INITCOND = '{0,0}'
);

-- Gini coefficient aggregate.  Measures inequality in a distribution.
-- For a sorted array of values x, the Gini coefficient is
--  (2 * Σ(i * x_i) / (n * Σ x_i)) - ((n + 1)/n).
CREATE OR REPLACE FUNCTION stats_extension._final_gini(
    arr numeric[]
) RETURNS numeric AS $$
DECLARE
    sorted      numeric[];
    n           integer;
    i           integer;
    sum_vals    numeric := 0;
    weighted_sum numeric := 0;
BEGIN
    IF arr IS NULL OR array_length(arr,1) IS NULL OR array_length(arr,1) = 0 THEN
        RETURN NULL;
    END IF;
    SELECT array_agg(val ORDER BY val) INTO sorted
    FROM unnest(arr) AS val;
    n := array_length(sorted,1);
    FOR i IN 1..n LOOP
        sum_vals    := sum_vals + sorted[i];
        weighted_sum := weighted_sum + sorted[i] * i;
    END LOOP;
    IF sum_vals = 0 THEN
        RETURN NULL;
    END IF;
    RETURN (2 * weighted_sum) / (n * sum_vals) - ((n + 1)::numeric / n);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE stats_extension.gini_coefficient(numeric) (
    SFUNC    = array_append,
    STYPE    = numeric[],
    FINALFUNC = stats_extension._final_gini,
    INITCOND = '{}'
);

-- Aggregate for trimmed mean using the custom composite state.  Supports
-- calling syntax such as SELECT stats_extension.trimmed_mean(value, 0.1) FROM ...;
CREATE AGGREGATE stats_extension.trimmed_mean(numeric, numeric) (
    SFUNC    = stats_extension.trimmed_mean_state,
    STYPE    = stats_extension_trim_state,
    FINALFUNC = stats_extension.trimmed_mean_final,
    INITCOND = '(NULL,0)'
);