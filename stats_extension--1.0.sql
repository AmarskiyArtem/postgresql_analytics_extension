-- Stats analytics extension for PostgreSQL

-- ---------------------------------------------------------------------------
-- Moment-based state tracking used by skewness and kurtosis aggregates
-- ---------------------------------------------------------------------------
CREATE TYPE stats_moment_state AS (
    n        bigint,
    sum_val  double precision,
    sum_sq   double precision,
    sum_cu   double precision,
    sum_qu   double precision
);

CREATE OR REPLACE FUNCTION stats_moment_state_transition(
    state stats_moment_state,
    value double precision
)
RETURNS stats_moment_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result stats_moment_state;
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;

    IF state IS NULL THEN
        result := ROW(0, 0, 0, 0, 0)::stats_moment_state;
    ELSE
        result := state;
    END IF;

    result.n := result.n + 1;
    result.sum_val := result.sum_val + value;
    result.sum_sq := result.sum_sq + value * value;
    result.sum_cu := result.sum_cu + value * value * value;
    result.sum_qu := result.sum_qu + value * value * value * value;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION stats_moment_state_transition_numeric(
    state stats_moment_state,
    value numeric
)
RETURNS stats_moment_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT stats_moment_state_transition($1, CASE WHEN $2 IS NULL THEN NULL ELSE $2::double precision END);
$$;

CREATE OR REPLACE FUNCTION stats_moment_state_combine(
    left_state stats_moment_state,
    right_state stats_moment_state
)
RETURNS stats_moment_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL THEN $2
    WHEN $2 IS NULL THEN $1
    ELSE ROW(
        $1.n + $2.n,
        $1.sum_val + $2.sum_val,
        $1.sum_sq + $2.sum_sq,
        $1.sum_cu + $2.sum_cu,
        $1.sum_qu + $2.sum_qu
    )::stats_moment_state
END;
$$;

CREATE OR REPLACE FUNCTION skewness_pop_final(state stats_moment_state)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    n double precision;
    mean double precision;
    m2 double precision;
    mu3 double precision;
    sigma double precision;
BEGIN
    IF state IS NULL OR state.n IS NULL OR state.n < 2 THEN
        RETURN NULL;
    END IF;

    n := state.n;
    mean := state.sum_val / n;
    m2 := state.sum_sq / n - mean * mean;

    IF m2 <= 0 THEN
        RETURN 0.0;
    END IF;

    mu3 := (state.sum_cu
        - 3 * mean * state.sum_sq
        + 3 * mean * mean * state.sum_val
        - n * mean * mean * mean) / n;

    sigma := sqrt(m2);
    IF sigma = 0 THEN
        RETURN 0.0;
    END IF;

    RETURN mu3 / (sigma * sigma * sigma);
END;
$$;

CREATE OR REPLACE FUNCTION skewness_samp_final(state stats_moment_state)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    n double precision;
    mean double precision;
    sum_central3 double precision;
    sum_sq_dev double precision;
    s double precision;
BEGIN
    IF state IS NULL OR state.n IS NULL OR state.n < 3 THEN
        RETURN NULL;
    END IF;

    n := state.n;
    mean := state.sum_val / n;
    sum_sq_dev := state.sum_sq - n * mean * mean;
    IF sum_sq_dev = 0 THEN
        RETURN 0.0;
    END IF;

    sum_central3 := state.sum_cu
        - 3 * mean * state.sum_sq
        + 3 * mean * mean * state.sum_val
        - n * mean * mean * mean;

    s := sqrt(sum_sq_dev / (n - 1));
    IF s = 0 THEN
        RETURN 0.0;
    END IF;

    RETURN (n * sum_central3) / ((n - 1) * (n - 2) * s * s * s);
END;
$$;

CREATE OR REPLACE FUNCTION kurtosis_pop_final(state stats_moment_state)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    n double precision;
    mean double precision;
    sum_sq_dev double precision;
    mu2 double precision;
    mu4 double precision;
BEGIN
    IF state IS NULL OR state.n IS NULL OR state.n < 2 THEN
        RETURN NULL;
    END IF;

    n := state.n;
    mean := state.sum_val / n;
    sum_sq_dev := state.sum_sq - n * mean * mean;
    mu2 := sum_sq_dev / n;

    IF mu2 = 0 THEN
        RETURN 0.0;
    END IF;

    mu4 := (state.sum_qu
        - 4 * mean * state.sum_cu
        + 6 * mean * mean * state.sum_sq
        - 4 * mean * mean * mean * state.sum_val
        + n * mean * mean * mean * mean) / n;

    RETURN mu4 / (mu2 * mu2) - 3.0;
END;
$$;

CREATE OR REPLACE FUNCTION kurtosis_samp_final(state stats_moment_state)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    n double precision;
    mean double precision;
    sum_sq_dev double precision;
    sum_central4 double precision;
    s2 double precision;
BEGIN
    IF state IS NULL OR state.n IS NULL OR state.n < 4 THEN
        RETURN NULL;
    END IF;

    n := state.n;
    mean := state.sum_val / n;
    sum_sq_dev := state.sum_sq - n * mean * mean;
    IF sum_sq_dev = 0 THEN
        RETURN 0.0;
    END IF;

    sum_central4 := state.sum_qu
        - 4 * mean * state.sum_cu
        + 6 * mean * mean * state.sum_sq
        - 4 * mean * mean * mean * state.sum_val
        + n * mean * mean * mean * mean;

    s2 := sum_sq_dev / (n - 1);
    IF s2 = 0 THEN
        RETURN 0.0;
    END IF;

    RETURN (n * (n + 1) * sum_central4) /
           ((n - 1) * (n - 2) * (n - 3) * s2 * s2)
           - 3 * (n - 1) * (n - 1) / ((n - 2) * (n - 3));
END;
$$;

-- ---------------------------------------------------------------------------
-- Array collection helpers used by several aggregates (median, mode, entropy)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION collect_float8_array(
    state double precision[],
    value double precision
)
RETURNS double precision[]
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $2 IS NULL THEN $1
    ELSE array_append(COALESCE($1, ARRAY[]::double precision[]), $2)
END;
$$;

CREATE OR REPLACE FUNCTION collect_float8_array_numeric(
    state double precision[],
    value numeric
)
RETURNS double precision[]
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT collect_float8_array($1, CASE WHEN $2 IS NULL THEN NULL ELSE $2::double precision END);
$$;

CREATE OR REPLACE FUNCTION concat_float8_array(
    left_state double precision[],
    right_state double precision[]
)
RETURNS double precision[]
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT COALESCE($1, ARRAY[]::double precision[]) || COALESCE($2, ARRAY[]::double precision[]);
$$;

CREATE OR REPLACE FUNCTION collect_text_array(
    state text[],
    value anyelement
)
RETURNS text[]
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;
    RETURN array_append(COALESCE(state, ARRAY[]::text[]), value::text);
END;
$$;

CREATE OR REPLACE FUNCTION concat_text_array(
    left_state text[],
    right_state text[]
)
RETURNS text[]
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT COALESCE($1, ARRAY[]::text[]) || COALESCE($2, ARRAY[]::text[]);
$$;

-- ---------------------------------------------------------------------------
-- Final functions for median, mode and entropy
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION median_float8_final(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    sorted double precision[];
    len integer;
    mid integer;
BEGIN
    IF state IS NULL OR array_length(state, 1) IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT array_agg(val ORDER BY val)
    INTO sorted
    FROM unnest(state) AS val;

    len := array_length(sorted, 1);
    IF len IS NULL OR len = 0 THEN
        RETURN NULL;
    END IF;

    IF len % 2 = 1 THEN
        mid := (len + 1) / 2;
        RETURN sorted[mid];
    ELSE
        mid := len / 2;
        RETURN (sorted[mid] + sorted[mid + 1]) / 2.0;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION mode_float8_final(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result double precision;
BEGIN
    IF state IS NULL OR array_length(state, 1) IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT val
    INTO result
    FROM (
        SELECT val, COUNT(*) AS cnt
        FROM unnest(state) AS val
        GROUP BY val
        ORDER BY cnt DESC, val ASC
        LIMIT 1
    ) ranked;

    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION entropy_text_array_final(state text[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    total double precision;
    entropy double precision := 0;
    rec RECORD;
BEGIN
    IF state IS NULL OR array_length(state, 1) IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT COUNT(*)::double precision INTO total
    FROM unnest(state) AS val;

    IF total = 0 THEN
        RETURN NULL;
    END IF;

    FOR rec IN
        SELECT val, COUNT(*)::double precision AS cnt
        FROM unnest(state) AS val
        GROUP BY val
    LOOP
        entropy := entropy - (rec.cnt / total) * (ln(rec.cnt / total) / ln(2));
    END LOOP;

    RETURN entropy;
END;
$$;

-- ---------------------------------------------------------------------------
-- Aggregate definitions for central moments, median, mode, and entropy
-- ---------------------------------------------------------------------------
CREATE AGGREGATE skewness_pop(double precision) (
    SFUNC = stats_moment_state_transition,
    STYPE = stats_moment_state,
    FINALFUNC = skewness_pop_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE skewness_pop(numeric) (
    SFUNC = stats_moment_state_transition_numeric,
    STYPE = stats_moment_state,
    FINALFUNC = skewness_pop_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE skewness_samp(double precision) (
    SFUNC = stats_moment_state_transition,
    STYPE = stats_moment_state,
    FINALFUNC = skewness_samp_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE skewness_samp(numeric) (
    SFUNC = stats_moment_state_transition_numeric,
    STYPE = stats_moment_state,
    FINALFUNC = skewness_samp_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE kurtosis_pop(double precision) (
    SFUNC = stats_moment_state_transition,
    STYPE = stats_moment_state,
    FINALFUNC = kurtosis_pop_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE kurtosis_pop(numeric) (
    SFUNC = stats_moment_state_transition_numeric,
    STYPE = stats_moment_state,
    FINALFUNC = kurtosis_pop_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE kurtosis_samp(double precision) (
    SFUNC = stats_moment_state_transition,
    STYPE = stats_moment_state,
    FINALFUNC = kurtosis_samp_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE kurtosis_samp(numeric) (
    SFUNC = stats_moment_state_transition_numeric,
    STYPE = stats_moment_state,
    FINALFUNC = kurtosis_samp_final,
    COMBINEFUNC = stats_moment_state_combine
);

CREATE AGGREGATE median(double precision) (
    SFUNC = collect_float8_array,
    STYPE = double precision[],
    FINALFUNC = median_float8_final,
    COMBINEFUNC = concat_float8_array,
    INITCOND = '{}'
);

CREATE AGGREGATE median(numeric) (
    SFUNC = collect_float8_array_numeric,
    STYPE = double precision[],
    FINALFUNC = median_float8_final,
    COMBINEFUNC = concat_float8_array,
    INITCOND = '{}'
);

CREATE AGGREGATE mode(double precision) (
    SFUNC = collect_float8_array,
    STYPE = double precision[],
    FINALFUNC = mode_float8_final,
    COMBINEFUNC = concat_float8_array,
    INITCOND = '{}'
);

CREATE AGGREGATE mode(numeric) (
    SFUNC = collect_float8_array_numeric,
    STYPE = double precision[],
    FINALFUNC = mode_float8_final,
    COMBINEFUNC = concat_float8_array,
    INITCOND = '{}'
);

CREATE AGGREGATE entropy(anyelement) (
    SFUNC = collect_text_array,
    STYPE = text[],
    FINALFUNC = entropy_text_array_final,
    COMBINEFUNC = concat_text_array,
    INITCOND = '{}'
);

-- ---------------------------------------------------------------------------
-- Weighted average and conditional average aggregates
-- ---------------------------------------------------------------------------
CREATE TYPE weighted_avg_state AS (
    sum_weighted double precision,
    sum_weight double precision
);

CREATE OR REPLACE FUNCTION weighted_avg_state_transition(
    state weighted_avg_state,
    value double precision,
    weight double precision
)
RETURNS weighted_avg_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result weighted_avg_state;
BEGIN
    IF value IS NULL OR weight IS NULL THEN
        RETURN state;
    END IF;

    IF state IS NULL THEN
        result := ROW(0, 0)::weighted_avg_state;
    ELSE
        result := state;
    END IF;

    result.sum_weighted := result.sum_weighted + value * weight;
    result.sum_weight := result.sum_weight + weight;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION weighted_avg_state_transition_numeric(
    state weighted_avg_state,
    value numeric,
    weight numeric
)
RETURNS weighted_avg_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT weighted_avg_state_transition(
    $1,
    CASE WHEN $2 IS NULL THEN NULL ELSE $2::double precision END,
    CASE WHEN $3 IS NULL THEN NULL ELSE $3::double precision END
);
$$;

CREATE OR REPLACE FUNCTION weighted_avg_final(state weighted_avg_state)
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL OR $1.sum_weight = 0 THEN NULL
    ELSE $1.sum_weighted / $1.sum_weight
END;
$$;

CREATE OR REPLACE FUNCTION weighted_avg_state_combine(
    left_state weighted_avg_state,
    right_state weighted_avg_state
)
RETURNS weighted_avg_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL THEN $2
    WHEN $2 IS NULL THEN $1
    ELSE ROW(
        $1.sum_weighted + $2.sum_weighted,
        $1.sum_weight + $2.sum_weight
    )::weighted_avg_state
END;
$$;

CREATE AGGREGATE avg_weighted(double precision, double precision) (
    SFUNC = weighted_avg_state_transition,
    STYPE = weighted_avg_state,
    FINALFUNC = weighted_avg_final,
    COMBINEFUNC = weighted_avg_state_combine
);

CREATE AGGREGATE avg_weighted(numeric, numeric) (
    SFUNC = weighted_avg_state_transition_numeric,
    STYPE = weighted_avg_state,
    FINALFUNC = weighted_avg_final,
    COMBINEFUNC = weighted_avg_state_combine
);

CREATE TYPE avg_if_state AS (
    sum_val double precision,
    cnt bigint
);

CREATE OR REPLACE FUNCTION avg_if_state_transition(
    state avg_if_state,
    value double precision,
    condition boolean
)
RETURNS avg_if_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result avg_if_state;
BEGIN
    IF condition IS DISTINCT FROM TRUE OR value IS NULL THEN
        RETURN state;
    END IF;

    IF state IS NULL THEN
        result := ROW(0, 0)::avg_if_state;
    ELSE
        result := state;
    END IF;

    result.sum_val := result.sum_val + value;
    result.cnt := result.cnt + 1;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION avg_if_state_transition_numeric(
    state avg_if_state,
    value numeric,
    condition boolean
)
RETURNS avg_if_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT avg_if_state_transition(
    $1,
    CASE WHEN $2 IS NULL THEN NULL ELSE $2::double precision END,
    $3
);
$$;

CREATE OR REPLACE FUNCTION avg_if_final(state avg_if_state)
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL OR $1.cnt = 0 THEN NULL
    ELSE $1.sum_val / $1.cnt
END;
$$;

CREATE OR REPLACE FUNCTION avg_if_state_combine(
    left_state avg_if_state,
    right_state avg_if_state
)
RETURNS avg_if_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL THEN $2
    WHEN $2 IS NULL THEN $1
    ELSE ROW(
        $1.sum_val + $2.sum_val,
        $1.cnt + $2.cnt
    )::avg_if_state
END;
$$;

CREATE AGGREGATE avg_if(double precision, boolean) (
    SFUNC = avg_if_state_transition,
    STYPE = avg_if_state,
    FINALFUNC = avg_if_final,
    COMBINEFUNC = avg_if_state_combine
);

CREATE AGGREGATE avg_if(numeric, boolean) (
    SFUNC = avg_if_state_transition_numeric,
    STYPE = avg_if_state,
    FINALFUNC = avg_if_final,
    COMBINEFUNC = avg_if_state_combine
);

-- ---------------------------------------------------------------------------
-- Geometric mean and product aggregates
-- ---------------------------------------------------------------------------
CREATE TYPE geo_mean_state AS (
    cnt bigint,
    log_sum double precision,
    zero_cnt bigint,
    has_negative boolean
);

CREATE OR REPLACE FUNCTION geo_mean_state_transition(
    state geo_mean_state,
    value double precision
)
RETURNS geo_mean_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result geo_mean_state;
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;

    IF state IS NULL THEN
        result := ROW(0, 0, 0, false)::geo_mean_state;
    ELSE
        result := state;
    END IF;

    IF value < 0 THEN
        result.has_negative := true;
    ELSIF value = 0 THEN
        result.zero_cnt := result.zero_cnt + 1;
    ELSE
        result.cnt := result.cnt + 1;
        result.log_sum := result.log_sum + ln(value);
    END IF;

    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION geo_mean_state_transition_numeric(
    state geo_mean_state,
    value numeric
)
RETURNS geo_mean_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT geo_mean_state_transition($1, CASE WHEN $2 IS NULL THEN NULL ELSE $2::double precision END);
$$;

CREATE OR REPLACE FUNCTION geo_mean_final(state geo_mean_state)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;

    IF state.has_negative THEN
        RETURN NULL;
    END IF;

    IF state.zero_cnt > 0 THEN
        RETURN 0.0;
    END IF;

    IF state.cnt = 0 THEN
        RETURN NULL;
    END IF;

    RETURN exp(state.log_sum / state.cnt);
END;
$$;

CREATE OR REPLACE FUNCTION geo_mean_state_combine(
    left_state geo_mean_state,
    right_state geo_mean_state
)
RETURNS geo_mean_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL THEN $2
    WHEN $2 IS NULL THEN $1
    ELSE ROW(
        $1.cnt + $2.cnt,
        $1.log_sum + $2.log_sum,
        $1.zero_cnt + $2.zero_cnt,
        $1.has_negative OR $2.has_negative
    )::geo_mean_state
END;
$$;

CREATE AGGREGATE geometric_mean(double precision) (
    SFUNC = geo_mean_state_transition,
    STYPE = geo_mean_state,
    FINALFUNC = geo_mean_final,
    COMBINEFUNC = geo_mean_state_combine
);

CREATE AGGREGATE geometric_mean(numeric) (
    SFUNC = geo_mean_state_transition_numeric,
    STYPE = geo_mean_state,
    FINALFUNC = geo_mean_final,
    COMBINEFUNC = geo_mean_state_combine
);

CREATE TYPE product_state AS (
    product double precision,
    has_value boolean
);

CREATE OR REPLACE FUNCTION product_state_transition(
    state product_state,
    value double precision
)
RETURNS product_state
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result product_state;
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;

    IF state IS NULL THEN
        result := ROW(1, false)::product_state;
    ELSE
        result := state;
    END IF;

    IF NOT result.has_value THEN
        result.product := value;
        result.has_value := true;
    ELSE
        result.product := result.product * value;
    END IF;

    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION product_state_transition_numeric(
    state product_state,
    value numeric
)
RETURNS product_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT product_state_transition($1, CASE WHEN $2 IS NULL THEN NULL ELSE $2::double precision END);
$$;

CREATE OR REPLACE FUNCTION product_final(state product_state)
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL OR NOT $1.has_value THEN NULL
    ELSE $1.product
END;
$$;

CREATE OR REPLACE FUNCTION product_state_combine(
    left_state product_state,
    right_state product_state
)
RETURNS product_state
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL THEN $2
    WHEN $2 IS NULL THEN $1
    WHEN NOT $1.has_value THEN $2
    WHEN NOT $2.has_value THEN $1
    ELSE ROW(
        $1.product * $2.product,
        true
    )::product_state
END;
$$;

CREATE AGGREGATE product(double precision) (
    SFUNC = product_state_transition,
    STYPE = product_state,
    FINALFUNC = product_final,
    COMBINEFUNC = product_state_combine
);

CREATE AGGREGATE product(numeric) (
    SFUNC = product_state_transition_numeric,
    STYPE = product_state,
    FINALFUNC = product_final,
    COMBINEFUNC = product_state_combine
);

CREATE OR REPLACE FUNCTION contingency_state_add(
    state jsonb,
    left_value text,
    right_value text,
    amount bigint
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    lkey text := COALESCE(left_value, '__NULL__');
    rkey text := COALESCE(right_value, '__NULL__');
    current jsonb;
    current_count bigint;
BEGIN
    IF amount IS NULL OR amount = 0 THEN
        RETURN state;
    END IF;

    IF state IS NULL THEN
        state := '{}'::jsonb;
    END IF;

    current := state -> lkey;
    IF current IS NULL THEN
        state := state || jsonb_build_object(lkey, jsonb_build_object(rkey, amount));
    ELSE
        current_count := COALESCE((current ->> rkey)::bigint, 0);
        current := current || jsonb_build_object(rkey, current_count + amount);
        state := state || jsonb_build_object(lkey, current);
    END IF;

    RETURN state;
END;
$$;

CREATE OR REPLACE FUNCTION contingency_state_transition(
    state jsonb,
    left_value text,
    right_value text
)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT contingency_state_add($1, $2, $3, 1);
$$;

CREATE OR REPLACE FUNCTION contingency_state_combine(
    left_state jsonb,
    right_state jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    rec RECORD;
    inner_rec RECORD;
BEGIN
    IF right_state IS NULL THEN
        RETURN left_state;
    END IF;
    IF left_state IS NULL THEN
        RETURN right_state;
    END IF;

    FOR rec IN SELECT key, value FROM jsonb_each(right_state)
    LOOP
        FOR inner_rec IN SELECT key, value FROM jsonb_each(rec.value)
        LOOP
            left_state := contingency_state_add(
                left_state,
                CASE WHEN rec.key = '__NULL__' THEN NULL ELSE rec.key END,
                CASE WHEN inner_rec.key = '__NULL__' THEN NULL ELSE inner_rec.key END,
                inner_rec.value::bigint
            );
        END LOOP;
    END LOOP;
    RETURN left_state;
END;
$$;

CREATE OR REPLACE FUNCTION contingency_state_transition_any(
    state jsonb,
    left_value anyelement,
    right_value anyelement
)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT contingency_state_transition(
    $1,
    CASE WHEN $2 IS NULL THEN NULL ELSE $2::text END,
    CASE WHEN $3 IS NULL THEN NULL ELSE $3::text END
);
$$;

CREATE OR REPLACE FUNCTION contingency_final(state jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    rec RECORD;
    inner_rec RECORD;
    result jsonb := '[]'::jsonb;
BEGIN
    IF state IS NULL OR state = '{}'::jsonb THEN
        RETURN NULL;
    END IF;

    FOR rec IN SELECT key, value FROM jsonb_each(state)
    LOOP
        FOR inner_rec IN SELECT key, value FROM jsonb_each(rec.value)
        LOOP
            result := result || jsonb_build_array(
                jsonb_build_object(
                    'left', CASE WHEN rec.key = '__NULL__' THEN NULL ELSE rec.key END,
                    'right', CASE WHEN inner_rec.key = '__NULL__' THEN NULL ELSE inner_rec.key END,
                    'count', inner_rec.value::bigint
                )
            );
        END LOOP;
    END LOOP;

    IF result = '[]'::jsonb THEN
        RETURN NULL;
    END IF;
    RETURN result;
END;
$$;

CREATE AGGREGATE contingency(anyelement, anyelement) (
    SFUNC = contingency_state_transition_any,
    STYPE = jsonb,
    FINALFUNC = contingency_final,
    COMBINEFUNC = contingency_state_combine,
    INITCOND = '{}'
);

-- ---------------------------------------------------------------------------
-- Scalar helpers: edit distance, similarity, checksum
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION edit_distance(
    left_input text,
    right_input text
)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    len_left integer;
    len_right integer;
    previous integer[];
    current integer[];
    i integer;
    j integer;
    cost integer;
BEGIN
    IF left_input IS NULL OR right_input IS NULL THEN
        RETURN NULL;
    END IF;

    len_left := char_length(left_input);
    len_right := char_length(right_input);

    IF len_left = 0 THEN
        RETURN len_right;
    ELSIF len_right = 0 THEN
        RETURN len_left;
    END IF;

    previous := ARRAY(SELECT g FROM generate_series(0, len_right) AS g);
    current := array_fill(0, ARRAY[len_right + 1]);

    FOR i IN 1..len_left LOOP
        current[1] := i;
        FOR j IN 1..len_right LOOP
            IF substr(left_input, i, 1) = substr(right_input, j, 1) THEN
                cost := 0;
            ELSE
                cost := 1;
            END IF;

            current[j + 1] := LEAST(
                current[j] + 1,
                previous[j + 1] + 1,
                previous[j] + cost
            );
        END LOOP;
        previous := current;
        IF i < len_left THEN
            current := array_fill(0, ARRAY[len_right + 1]);
        END IF;
    END LOOP;

    RETURN previous[len_right + 1];
END;
$$;

CREATE OR REPLACE FUNCTION edit_distance_similarity(
    left_input text,
    right_input text
)
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT CASE
    WHEN $1 IS NULL OR $2 IS NULL THEN NULL
    ELSE
        CASE
            WHEN GREATEST(char_length($1), char_length($2)) = 0 THEN 1.0
            ELSE 1.0 - edit_distance($1, $2)::double precision
                      / GREATEST(char_length($1), char_length($2))
        END
END;
$$;

CREATE OR REPLACE FUNCTION checksum(VARIADIC elements text[])
RETURNS bigint
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    result bigint := 0;
    element text;
BEGIN
    IF elements IS NULL THEN
        RETURN NULL;
    END IF;

    IF array_length(elements, 1) IS NULL THEN
        RETURN 0;
    END IF;

    FOREACH element IN ARRAY elements LOOP
        result := ((result * 31) # pg_catalog.hashtextextended(COALESCE(element, '__NULL__'), 0));
    END LOOP;

    RETURN result;
END;
$$;
