-- stats_extension--1.0.sql
-- Collection of analytic helpers implemented with plain SQL/PL/pgSQL so the
-- extension can be built without compiling C code.

------------------------------------------------------------------------------
-- Moment helpers (used by skewness/kurtosis)
------------------------------------------------------------------------------

CREATE FUNCTION stats_extension__moment_state_transition(state double precision[], value double precision)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    next_state double precision[];
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;

    next_state := state;
    IF next_state IS NULL THEN
        next_state := ARRAY[0::double precision, 0::double precision, 0::double precision, 0::double precision, 0::double precision];
    END IF;

    next_state[1] := next_state[1] + 1;
    next_state[2] := next_state[2] + value;
    next_state[3] := next_state[3] + value * value;
    next_state[4] := next_state[4] + value * value * value;
    next_state[5] := next_state[5] + value * value * value * value;

    RETURN next_state;
END;
$$;

CREATE FUNCTION stats_extension__moment_state_combine(state_a double precision[], state_b double precision[])
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    result double precision[];
BEGIN
    IF state_a IS NULL THEN
        RETURN state_b;
    ELSIF state_b IS NULL THEN
        RETURN state_a;
    END IF;

    result := ARRAY[state_a[1] + state_b[1],
                    state_a[2] + state_b[2],
                    state_a[3] + state_b[3],
                    state_a[4] + state_b[4],
                    state_a[5] + state_b[5]];
    RETURN result;
END;
$$;

CREATE FUNCTION stats_extension__central_m2(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    n double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;

    n := state[1];
    IF n = 0 THEN
        RETURN NULL;
    END IF;
    RETURN state[3] - (state[2] * state[2]) / n;
END;
$$;

CREATE FUNCTION stats_extension__central_m3(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    n double precision;
    mean double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;

    n := state[1];
    IF n = 0 THEN
        RETURN NULL;
    END IF;

    mean := state[2] / n;
    RETURN state[4]
           - 3 * mean * state[3]
           + 3 * mean * mean * state[2]
           - n * mean * mean * mean;
END;
$$;

CREATE FUNCTION stats_extension__central_m4(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    n double precision;
    mean double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;

    n := state[1];
    IF n = 0 THEN
        RETURN NULL;
    END IF;

    mean := state[2] / n;
    RETURN state[5]
           - 4 * mean * state[4]
           + 6 * mean * mean * state[3]
           - 4 * mean * mean * mean * state[2]
           + n * mean * mean * mean * mean;
END;
$$;

CREATE FUNCTION stats_extension__skewness_pop_final(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    n double precision;
    central_m2 double precision;
    central_m3 double precision;
    mu2 double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;
    n := state[1];
    IF n = 0 THEN
        RETURN NULL;
    END IF;

    central_m2 := stats_extension__central_m2(state);
    IF central_m2 IS NULL OR central_m2 = 0 THEN
        RETURN NULL;
    END IF;
    mu2 := central_m2 / n;

    central_m3 := stats_extension__central_m3(state);
    RETURN (central_m3 / n) / power(mu2, 1.5);
END;
$$;

CREATE FUNCTION stats_extension__skewness_samp_final(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    n double precision;
    central_m2 double precision;
    central_m3 double precision;
    denom double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;
    n := state[1];
    IF n < 3 THEN
        RETURN NULL;
    END IF;

    central_m2 := stats_extension__central_m2(state);
    IF central_m2 IS NULL OR central_m2 = 0 THEN
        RETURN NULL;
    END IF;
    central_m3 := stats_extension__central_m3(state);
    denom := (n - 1) * (n - 2) * power(central_m2 / (n - 1), 1.5);
    IF denom = 0 THEN
        RETURN NULL;
    END IF;
    RETURN (n * central_m3) / denom;
END;
$$;

CREATE FUNCTION stats_extension__kurtosis_pop_final(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    n double precision;
    central_m2 double precision;
    central_m4 double precision;
    mu2 double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;
    n := state[1];
    IF n = 0 THEN
        RETURN NULL;
    END IF;

    central_m2 := stats_extension__central_m2(state);
    IF central_m2 IS NULL OR central_m2 = 0 THEN
        RETURN NULL;
    END IF;
    central_m4 := stats_extension__central_m4(state);
    mu2 := central_m2 / n;
    RETURN (central_m4 / n) / (mu2 * mu2) - 3;
END;
$$;

CREATE FUNCTION stats_extension__kurtosis_samp_final(state double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    n double precision;
    central_m2 double precision;
    central_m4 double precision;
    s2 double precision;
    numerator double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;
    n := state[1];
    IF n < 4 THEN
        RETURN NULL;
    END IF;

    central_m2 := stats_extension__central_m2(state);
    IF central_m2 IS NULL OR central_m2 = 0 THEN
        RETURN NULL;
    END IF;
    central_m4 := stats_extension__central_m4(state);
    s2 := central_m2 / (n - 1);
    IF s2 = 0 THEN
        RETURN NULL;
    END IF;

    numerator := n * (n + 1) * central_m4 / ((n - 1) * (n - 2) * (n - 3) * s2 * s2);
    RETURN numerator - (3 * (n - 1) * (n - 1)) / ((n - 2) * (n - 3));
END;
$$;

CREATE AGGREGATE skewness_pop(double precision)
(
    SFUNC = stats_extension__moment_state_transition,
    STYPE = double precision[],
    FINALFUNC = stats_extension__skewness_pop_final,
    COMBINEFUNC = stats_extension__moment_state_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE skewness_samp(double precision)
(
    SFUNC = stats_extension__moment_state_transition,
    STYPE = double precision[],
    FINALFUNC = stats_extension__skewness_samp_final,
    COMBINEFUNC = stats_extension__moment_state_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE kurtosis_pop(double precision)
(
    SFUNC = stats_extension__moment_state_transition,
    STYPE = double precision[],
    FINALFUNC = stats_extension__kurtosis_pop_final,
    COMBINEFUNC = stats_extension__moment_state_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE kurtosis_samp(double precision)
(
    SFUNC = stats_extension__moment_state_transition,
    STYPE = double precision[],
    FINALFUNC = stats_extension__kurtosis_samp_final,
    COMBINEFUNC = stats_extension__moment_state_combine,
    PARALLEL = SAFE
);

------------------------------------------------------------------------------
-- Median helpers
------------------------------------------------------------------------------

CREATE FUNCTION stats_extension__array_collect_float8(state double precision[], value double precision)
RETURNS double precision[]
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN $2 IS NULL THEN $1
         WHEN $1 IS NULL THEN ARRAY[$2]
         ELSE array_append($1, $2)
       END;
$$;

CREATE FUNCTION stats_extension__array_collect_numeric(state numeric[], value numeric)
RETURNS numeric[]
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN $2 IS NULL THEN $1
         WHEN $1 IS NULL THEN ARRAY[$2]
         ELSE array_append($1, $2)
       END;
$$;

CREATE FUNCTION stats_extension__median_float8_final(values double precision[])
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    sorted double precision[];
    length integer;
BEGIN
    IF values IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT array_agg(val ORDER BY val)
      INTO sorted
      FROM unnest(values) AS val;
    length := COALESCE(array_length(sorted, 1), 0);
    IF length = 0 THEN
        RETURN NULL;
    ELSIF length % 2 = 1 THEN
        RETURN sorted[(length + 1) / 2];
    ELSE
        RETURN (sorted[length / 2] + sorted[length / 2 + 1]) / 2.0;
    END IF;
END;
$$;

CREATE FUNCTION stats_extension__median_numeric_final(values numeric[])
RETURNS numeric
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    sorted numeric[];
    length integer;
BEGIN
    IF values IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT array_agg(val ORDER BY val)
      INTO sorted
      FROM unnest(values) AS val;
    length := COALESCE(array_length(sorted, 1), 0);
    IF length = 0 THEN
        RETURN NULL;
    ELSIF length % 2 = 1 THEN
        RETURN sorted[(length + 1) / 2];
    ELSE
        RETURN (sorted[length / 2] + sorted[length / 2 + 1]) / 2;
    END IF;
END;
$$;

CREATE AGGREGATE median(double precision)
(
    SFUNC = stats_extension__array_collect_float8,
    STYPE = double precision[],
    FINALFUNC = stats_extension__median_float8_final,
    PARALLEL = SAFE
);

CREATE AGGREGATE median(numeric)
(
    SFUNC = stats_extension__array_collect_numeric,
    STYPE = numeric[],
    FINALFUNC = stats_extension__median_numeric_final,
    PARALLEL = SAFE
);

------------------------------------------------------------------------------
-- Mode / entropy helpers that rely on jsonb counters
------------------------------------------------------------------------------

CREATE FUNCTION stats_extension__jsonb_counter(state jsonb, value anyelement)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    key text;
    current bigint;
    next_state jsonb;
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;
    key := format('%s', value);
    next_state := COALESCE(state, '{}'::jsonb);
    current := COALESCE((next_state ->> key)::bigint, 0);
    next_state := next_state || jsonb_build_object(key, to_jsonb(current + 1));
    RETURN next_state;
END;
$$;

CREATE FUNCTION stats_extension__jsonb_counter_combine(state_a jsonb, state_b jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    record_entry record;
    accumulated jsonb;
    new_count bigint;
BEGIN
    IF state_a IS NULL THEN
        RETURN state_b;
    ELSIF state_b IS NULL THEN
        RETURN state_a;
    END IF;
    accumulated := state_a;
    FOR record_entry IN
        SELECT key, value::bigint AS count_value
          FROM jsonb_each_text(state_b)
    LOOP
        new_count := COALESCE((accumulated ->> record_entry.key)::bigint, 0) + record_entry.count_value;
        accumulated := accumulated || jsonb_build_object(record_entry.key, to_jsonb(new_count));
    END LOOP;
    RETURN accumulated;
END;
$$;

CREATE FUNCTION stats_extension__mode_pick_key(state jsonb)
RETURNS text
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT key
      FROM (
            SELECT key, value::numeric AS freq
              FROM jsonb_each_text(COALESCE(state, '{}'::jsonb))
           ) counted
  ORDER BY counted.freq DESC, counted.key
     LIMIT 1;
$$;

CREATE FUNCTION stats_extension__mode_text_final(state jsonb)
RETURNS text
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT stats_extension__mode_pick_key(COALESCE($1, '{}'::jsonb));
$$;

CREATE FUNCTION stats_extension__mode_float8_final(state jsonb)
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN key IS NULL THEN NULL
         ELSE key::double precision
       END
  FROM (SELECT stats_extension__mode_pick_key(COALESCE($1, '{}'::jsonb)) AS key) s;
$$;

CREATE FUNCTION stats_extension__mode_numeric_final(state jsonb)
RETURNS numeric
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN key IS NULL THEN NULL
         ELSE key::numeric
       END
  FROM (SELECT stats_extension__mode_pick_key(COALESCE($1, '{}'::jsonb)) AS key) s;
$$;

CREATE AGGREGATE mode(text)
(
    SFUNC = stats_extension__jsonb_counter,
    STYPE = jsonb,
    FINALFUNC = stats_extension__mode_text_final,
    COMBINEFUNC = stats_extension__jsonb_counter_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE mode(double precision)
(
    SFUNC = stats_extension__jsonb_counter,
    STYPE = jsonb,
    FINALFUNC = stats_extension__mode_float8_final,
    COMBINEFUNC = stats_extension__jsonb_counter_combine,
    PARALLEL = SAFE
);

CREATE AGGREGATE mode(numeric)
(
    SFUNC = stats_extension__jsonb_counter,
    STYPE = jsonb,
    FINALFUNC = stats_extension__mode_numeric_final,
    COMBINEFUNC = stats_extension__jsonb_counter_combine,
    PARALLEL = SAFE
);

CREATE FUNCTION entropy_state(jsonb, anyelement)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT stats_extension__jsonb_counter($1, $2);
$$;

CREATE FUNCTION entropy_state_combine(jsonb, jsonb)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT stats_extension__jsonb_counter_combine($1, $2);
$$;

CREATE FUNCTION entropy_final(state jsonb)
RETURNS double precision
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    total double precision := 0;
    probability double precision;
    freq double precision;
    result double precision := 0;
BEGIN
    IF state IS NULL OR jsonb_object_length(state) = 0 THEN
        RETURN NULL;
    END IF;

    FOR freq IN SELECT value::double precision FROM jsonb_each_text(state)
    LOOP
        total := total + freq;
    END LOOP;

    IF total = 0 THEN
        RETURN NULL;
    END IF;

    FOR freq IN SELECT value::double precision FROM jsonb_each_text(state)
    LOOP
        probability := freq / total;
        IF probability > 0 THEN
            result := result - probability * (ln(probability) / ln(2.0));
        END IF;
    END LOOP;

    RETURN result;
END;
$$;

CREATE AGGREGATE entropy(anyelement)
(
    SFUNC = entropy_state,
    STYPE = jsonb,
    FINALFUNC = entropy_final,
    COMBINEFUNC = entropy_state_combine,
    PARALLEL = SAFE
);

------------------------------------------------------------------------------
-- Avg weighted / avg if
------------------------------------------------------------------------------

CREATE FUNCTION stats_extension__avg_weighted_state(state double precision[], value double precision, weight double precision)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    next_state double precision[];
BEGIN
    IF value IS NULL OR weight IS NULL THEN
        RETURN state;
    END IF;
    next_state := COALESCE(state, ARRAY[0::double precision, 0::double precision]);
    next_state[1] := next_state[1] + value * weight;
    next_state[2] := next_state[2] + weight;
    RETURN next_state;
END;
$$;

CREATE FUNCTION stats_extension__avg_weighted_final(state double precision[])
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN $1 IS NULL OR $1[2] = 0 THEN NULL
         ELSE $1[1] / $1[2]
       END;
$$;

CREATE AGGREGATE avg_weighted(value double precision, weight double precision)
(
    SFUNC = stats_extension__avg_weighted_state,
    STYPE = double precision[],
    FINALFUNC = stats_extension__avg_weighted_final,
    PARALLEL = SAFE
);

CREATE FUNCTION stats_extension__avg_if_state(state double precision[], value double precision, condition boolean)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    next_state double precision[];
BEGIN
    IF condition IS DISTINCT FROM TRUE OR value IS NULL THEN
        RETURN state;
    END IF;
    next_state := COALESCE(state, ARRAY[0::double precision, 0::double precision]);
    next_state[1] := next_state[1] + value;
    next_state[2] := next_state[2] + 1;
    RETURN next_state;
END;
$$;

CREATE FUNCTION stats_extension__avg_if_final(state double precision[])
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN $1 IS NULL OR $1[2] = 0 THEN NULL
         ELSE $1[1] / $1[2]
       END;
$$;

CREATE AGGREGATE avg_if(value double precision, condition boolean)
(
    SFUNC = stats_extension__avg_if_state,
    STYPE = double precision[],
    FINALFUNC = stats_extension__avg_if_final,
    PARALLEL = SAFE
);

------------------------------------------------------------------------------
-- Geometric mean and product
------------------------------------------------------------------------------

CREATE FUNCTION stats_extension__geom_mean_state(state double precision[], value double precision)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    next_state double precision[];
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;
    next_state := COALESCE(state, ARRAY[0::double precision, 0::double precision, 0::double precision]);
    IF value <= 0 THEN
        next_state[3] := 1;
        RETURN next_state;
    END IF;
    next_state[1] := next_state[1] + 1;
    next_state[2] := next_state[2] + ln(value);
    RETURN next_state;
END;
$$;

CREATE FUNCTION stats_extension__geom_mean_final(state double precision[])
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN $1 IS NULL OR $1[1] = 0 OR $1[3] <> 0 THEN NULL
         ELSE exp($1[2] / $1[1])
       END;
$$;

CREATE AGGREGATE geometric_mean(double precision)
(
    SFUNC = stats_extension__geom_mean_state,
    STYPE = double precision[],
    FINALFUNC = stats_extension__geom_mean_final,
    PARALLEL = SAFE
);

CREATE FUNCTION stats_extension__product_state(state double precision[], value double precision)
RETURNS double precision[]
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    next_state double precision[];
BEGIN
    IF value IS NULL THEN
        RETURN state;
    END IF;
    next_state := COALESCE(state, ARRAY[0::double precision, 1::double precision]);
    next_state[1] := next_state[1] + 1;
    next_state[2] := next_state[2] * value;
    RETURN next_state;
END;
$$;

CREATE FUNCTION stats_extension__product_final(state double precision[])
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT CASE
         WHEN $1 IS NULL OR $1[1] = 0 THEN NULL
         ELSE $1[2]
       END;
$$;

CREATE AGGREGATE product(double precision)
(
    SFUNC = stats_extension__product_state,
    STYPE = double precision[],
    FINALFUNC = stats_extension__product_final,
    PARALLEL = SAFE
);

------------------------------------------------------------------------------
-- Contingency table (ClickHouse-like)
------------------------------------------------------------------------------

CREATE FUNCTION stats_extension__contingency_state(state jsonb, row_value anyelement, column_value anyelement)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    row_key text;
    column_key text;
    table_state jsonb;
    row_state jsonb;
    current bigint;
BEGIN
    IF row_value IS NULL OR column_value IS NULL THEN
        RETURN state;
    END IF;
    row_key := format('%s', row_value);
    column_key := format('%s', column_value);
    table_state := COALESCE(state, '{}'::jsonb);
    row_state := COALESCE(table_state -> row_key, '{}'::jsonb);
    current := COALESCE((row_state ->> column_key)::bigint, 0);
    row_state := row_state || jsonb_build_object(column_key, to_jsonb(current + 1));
    table_state := table_state || jsonb_build_object(row_key, row_state);
    RETURN table_state;
END;
$$;

CREATE FUNCTION stats_extension__contingency_combine(state_a jsonb, state_b jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    row_entry record;
    col_entry record;
    table_state jsonb;
    row_state jsonb;
    new_count bigint;
BEGIN
    IF state_a IS NULL THEN
        RETURN state_b;
    ELSIF state_b IS NULL THEN
        RETURN state_a;
    END IF;

    table_state := state_a;
    FOR row_entry IN SELECT key, value FROM jsonb_each(state_b)
    LOOP
        row_state := COALESCE(table_state -> row_entry.key, '{}'::jsonb);
        FOR col_entry IN SELECT key, value FROM jsonb_each(row_entry.value)
        LOOP
            new_count := COALESCE((row_state ->> col_entry.key)::bigint, 0) + (col_entry.value)::text::bigint;
            row_state := row_state || jsonb_build_object(col_entry.key, to_jsonb(new_count));
        END LOOP;
        table_state := table_state || jsonb_build_object(row_entry.key, row_state);
    END LOOP;
    RETURN table_state;
END;
$$;

CREATE FUNCTION stats_extension__contingency_final(state jsonb)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT COALESCE($1, '{}'::jsonb);
$$;

CREATE AGGREGATE contingency(row_value anyelement, column_value anyelement)
(
    SFUNC = stats_extension__contingency_state,
    STYPE = jsonb,
    FINALFUNC = stats_extension__contingency_final,
    COMBINEFUNC = stats_extension__contingency_combine,
    PARALLEL = SAFE
);

------------------------------------------------------------------------------
-- Edit distance helpers (SQL Server-like)
------------------------------------------------------------------------------

CREATE FUNCTION edit_distance(left_text text, right_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
DECLARE
    len_left integer := length(left_text);
    len_right integer := length(right_text);
    previous integer[];
    current integer[];
    i integer;
    j integer;
    cost integer;
BEGIN
    IF len_left = 0 THEN
        RETURN len_right;
    ELSIF len_right = 0 THEN
        RETURN len_left;
    END IF;

    previous := ARRAY_FILL(0, ARRAY[len_right + 1]);
    FOR j IN 0..len_right LOOP
        previous[j + 1] := j;
    END LOOP;

    FOR i IN 1..len_left LOOP
        current := ARRAY_FILL(0, ARRAY[len_right + 1]);
        current[1] := i;
        FOR j IN 1..len_right LOOP
            IF substr(left_text, i, 1) = substr(right_text, j, 1) THEN
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
    END LOOP;

    RETURN previous[len_right + 1];
END;
$$;

CREATE FUNCTION edit_distance_similarity(left_text text, right_text text)
RETURNS double precision
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
WITH lengths AS (
    SELECT GREATEST(length(left_text), length(right_text)) AS max_len
)
SELECT CASE
         WHEN max_len = 0 THEN 1.0
         ELSE 1.0 - (edit_distance(left_text, right_text)::double precision / max_len)
       END
  FROM lengths;
$$;

------------------------------------------------------------------------------
-- Checksum (SQL Server-like)
------------------------------------------------------------------------------

CREATE FUNCTION stats_extension__checksum_text(value anyelement)
RETURNS text
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
SELECT COALESCE(format('%s', $1), '__stats_extension_null__');
$$;

CREATE FUNCTION checksum(VARIADIC values anyarray)
RETURNS bigint
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    idx integer;
    lower_idx integer;
    upper_idx integer;
    accumulator bigint := 0;
    token text;
BEGIN
    IF values IS NULL THEN
        RETURN NULL;
    END IF;

    lower_idx := array_lower(values, 1);
    upper_idx := array_upper(values, 1);

    IF lower_idx IS NULL THEN
        RETURN 0;
    END IF;

    FOR idx IN lower_idx..upper_idx LOOP
        token := stats_extension__checksum_text(values[idx]);
        accumulator := accumulator # hashtextextended(token, 0);
    END LOOP;

    RETURN accumulator;
END;
$$;
