CREATE OR REPLACE FUNCTION _skewness_pop_array(vals double precision[])
RETURNS double precision AS
$$
DECLARE
    n integer;
    mean_value double precision;
    sd double precision;
    s double precision := 0;
    v double precision;
BEGIN
    SELECT count(val), avg(val), stddev_pop(val)
      INTO n, mean_value, sd
      FROM unnest(vals) AS val
      WHERE val IS NOT NULL;
    IF n = 0 THEN
        RETURN NULL;
    END IF;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val WHERE val IS NOT NULL LOOP
        s := s + power((v - mean_value) / sd, 3);
    END LOOP;
    RETURN s / n;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION _skewness_samp_array(vals double precision[])
RETURNS double precision AS
$$
DECLARE
    n integer;
    mean_value double precision;
    sd double precision;
    s double precision := 0;
    v double precision;
BEGIN
    SELECT count(val), avg(val), stddev_samp(val)
      INTO n, mean_value, sd
      FROM unnest(vals) AS val
      WHERE val IS NOT NULL;
    IF n < 3 THEN
        RETURN NULL;
    END IF;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val WHERE val IS NOT NULL LOOP
        s := s + power((v - mean_value) / sd, 3);
    END LOOP;
    RETURN (n::double precision / ((n - 1) * (n - 2))) * s;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION _kurtosis_pop_array(vals double precision[])
RETURNS double precision AS
$$
DECLARE
    n integer;
    mean_value double precision;
    sd double precision;
    m4 double precision := 0;
    v double precision;
BEGIN
    SELECT count(val), avg(val), stddev_pop(val)
      INTO n, mean_value, sd
      FROM unnest(vals) AS val
      WHERE val IS NOT NULL;
    IF n = 0 THEN
        RETURN NULL;
    END IF;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val WHERE val IS NOT NULL LOOP
        m4 := m4 + power((v - mean_value) / sd, 4);
    END LOOP;
    RETURN m4 / n - 3;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION _kurtosis_samp_array(vals double precision[])
RETURNS double precision AS
$$
DECLARE
    n integer;
    mean_value double precision;
    sd double precision;
    m4 double precision := 0;
    v double precision;
    factor double precision;
BEGIN
    SELECT count(val), avg(val), stddev_samp(val)
      INTO n, mean_value, sd
      FROM unnest(vals) AS val
      WHERE val IS NOT NULL;
    IF n < 4 THEN
        RETURN NULL;
    END IF;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val WHERE val IS NOT NULL LOOP
        m4 := m4 + power((v - mean_value) / sd, 4);
    END LOOP;
    factor := ((n * (n + 1))::double precision / ((n - 1) * (n - 2) * (n - 3)));
    RETURN factor * m4 - (3 * power(n - 1, 2)::double precision) / ((n - 2) * (n - 3));
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE AGGREGATE skewness_pop(double precision) (
    STYPE    double precision[],
    SFUNC    array_append,
    FINALFUNC     _skewness_pop_array,
    INITCOND      '{}'
);

CREATE OR REPLACE AGGREGATE skewness_samp(double precision) (
    STYPE    double precision[],
    SFUNC    array_append,
    FINALFUNC     _skewness_samp_array,
    INITCOND      '{}'
);

CREATE OR REPLACE AGGREGATE kurtosis_pop(double precision) (
    STYPE    double precision[],
    SFUNC    array_append,
    FINALFUNC     _kurtosis_pop_array,
    INITCOND      '{}'
);

CREATE OR REPLACE AGGREGATE kurtosis_samp(double precision) (
    STYPE    double precision[],
    SFUNC    array_append,
    FINALFUNC     _kurtosis_samp_array,
    INITCOND      '{}'
);


CREATE OR REPLACE FUNCTION median_final(values anyarray)
RETURNS anyelement AS
$$
BEGIN
    RETURN (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
            FROM unnest(values) AS v
            WHERE v IS NOT NULL);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE median(anyelement) (
    STYPE      anyarray,
    SFUNC      array_append,
    FINALFUNC  median_final
);


CREATE OR REPLACE FUNCTION mode_final(values anyarray)
RETURNS anyelement AS
$$
DECLARE
    rec record;
BEGIN
    SELECT val
    INTO rec
    FROM (
        SELECT v AS val, count(*) AS cnt
        FROM unnest(values) AS v
        WHERE v IS NOT NULL
        GROUP BY v
        ORDER BY cnt DESC, val
        LIMIT 1
    ) AS sub;
    IF rec IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN rec.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE mode(anyelement) (
    STYPE      anyarray,
    SFUNC      array_append,
    FINALFUNC  mode_final
);


CREATE OR REPLACE FUNCTION entropy_final(values anyarray)
RETURNS double precision AS
$$
DECLARE
    total integer;
    entropy double precision := 0;
    rec record;
BEGIN
    SELECT count(*) INTO total
      FROM unnest(values) AS v
      WHERE v IS NOT NULL;
    IF total = 0 THEN
        RETURN NULL;
    END IF;
    FOR rec IN SELECT v AS val, count(*)::double precision AS cnt
               FROM unnest(values) AS v
               WHERE v IS NOT NULL
               GROUP BY v LOOP
        entropy := entropy - (rec.cnt/total) * ln(rec.cnt/total);
    END LOOP;
    RETURN entropy;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE entropy(anyelement) (
    STYPE      anyarray,
    SFUNC      array_append,
    FINALFUNC  entropy_final
);


CREATE OR REPLACE FUNCTION avg_weighted_trans(state double precision[], value double precision, weight double precision)
RETURNS double precision[] AS
$$
BEGIN
    IF state IS NULL OR array_length(state, 1) < 2 THEN
        state := ARRAY[0::double precision, 0::double precision];
    END IF;
    IF weight IS NOT NULL AND value IS NOT NULL THEN
        state[1] := state[1] + (value * weight);
        state[2] := state[2] + weight;
    END IF;
    RETURN state;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION avg_weighted_final(state double precision[])
RETURNS double precision AS
$$
BEGIN
    IF state IS NULL OR state[2] IS NULL OR state[2] = 0 THEN
        RETURN NULL;
    END IF;
    RETURN state[1] / state[2];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE avg_weighted(value double precision, weight double precision) (
    STYPE     double precision[],
    SFUNC     avg_weighted_trans,
    FINALFUNC avg_weighted_final,
    INITCOND  '{0,0}'
);


CREATE OR REPLACE FUNCTION avg_if_trans(state double precision[], value double precision, cond boolean)
RETURNS double precision[] AS
$$
BEGIN
    IF state IS NULL OR array_length(state, 1) < 2 THEN
        state := ARRAY[0::double precision, 0::double precision];
    END IF;
    IF cond IS TRUE AND value IS NOT NULL THEN
        state[1] := state[1] + value;
        state[2] := state[2] + 1;
    END IF;
    RETURN state;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION avg_if_final(state double precision[])
RETURNS double precision AS
$$
BEGIN
    IF state IS NULL OR state[2] IS NULL OR state[2] = 0 THEN
        RETURN NULL;
    END IF;
    RETURN state[1] / state[2];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE avg_if(value double precision, cond boolean) (
    STYPE     double precision[],
    SFUNC     avg_if_trans,
    FINALFUNC avg_if_final,
    INITCOND  '{0,0}'
);


CREATE OR REPLACE FUNCTION contingency_trans(state jsonb, val1 anyelement, val2 anyelement)
RETURNS jsonb AS
$$
DECLARE
    total integer;
    rows jsonb;
    cols jsonb;
    cells jsonb;
    r text;
    c text;
BEGIN
    IF state IS NULL THEN
        state := '{"total":0,"rows":{},"cols":{},"cells":{}}'::jsonb;
    END IF;

    IF val1 IS NULL OR val2 IS NULL THEN
        RETURN state;
    END IF;

    rows := COALESCE(state->'rows', '{}'::jsonb);
    cols := COALESCE(state->'cols', '{}'::jsonb);
    cells := COALESCE(state->'cells', '{}'::jsonb);
    total := COALESCE((state->>'total')::integer, 0) + 1;

    r := val1::text;
    c := val2::text;

    rows := rows || jsonb_build_object(r, COALESCE((rows->>r)::integer, 0) + 1);
    cols := cols || jsonb_build_object(c, COALESCE((cols->>c)::integer, 0) + 1);
    cells := cells || jsonb_build_object(r || '|' || c, COALESCE((cells->>(r || '|' || c))::integer, 0) + 1);

    RETURN jsonb_build_object(
        'total', total,
        'rows', rows,
        'cols', cols,
        'cells', cells
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION contingency_final(state jsonb)
RETURNS double precision AS
$$
DECLARE
    total double precision;
    rows jsonb;
    cols jsonb;
    cells jsonb;
    row_rec record;
    col_rec record;
    chi2 double precision := 0;
    row_count integer;
    col_count integer;
    denom double precision;
    observed double precision;
    expected double precision;
BEGIN
    IF state IS NULL THEN
        RETURN NULL;
    END IF;

    total := COALESCE((state->>'total')::double precision, 0);
    IF total = 0 THEN
        RETURN NULL;
    END IF;

    rows := COALESCE(state->'rows', '{}'::jsonb);
    cols := COALESCE(state->'cols', '{}'::jsonb);
    cells := COALESCE(state->'cells', '{}'::jsonb);

    SELECT count(*) INTO row_count FROM jsonb_object_keys(rows) AS row_keys(key);
    SELECT count(*) INTO col_count FROM jsonb_object_keys(cols) AS col_keys(key);

    IF row_count < 2 OR col_count < 2 THEN
        RETURN 0;
    END IF;

    FOR row_rec IN SELECT key, value FROM jsonb_each_text(rows) AS r(key, value) LOOP
        FOR col_rec IN SELECT key, value FROM jsonb_each_text(cols) AS c(key, value) LOOP
            observed := COALESCE((cells->>(row_rec.key || '|' || col_rec.key))::double precision, 0);
            expected := (row_rec.value::double precision * col_rec.value::double precision) / total;
            IF expected > 0 THEN
                chi2 := chi2 + power(observed - expected, 2) / expected;
            END IF;
        END LOOP;
    END LOOP;

    denom := total * LEAST(row_count - 1, col_count - 1);
    IF denom = 0 THEN
        RETURN 0;
    END IF;

    RETURN sqrt(chi2 / denom);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE contingency(val1 anyelement, val2 anyelement) (
    STYPE      jsonb,
    SFUNC      contingency_trans,
    FINALFUNC  contingency_final
);


CREATE OR REPLACE FUNCTION geometric_mean_final(values double precision[])
RETURNS double precision AS
$$
DECLARE
    n integer;
    sumlog double precision := 0;
    v double precision;
BEGIN
    SELECT count(val) INTO n
      FROM unnest(values) AS val
      WHERE val IS NOT NULL;
    IF n = 0 THEN
        RETURN NULL;
    END IF;
    FOR v IN SELECT val FROM unnest(values) AS val WHERE val IS NOT NULL LOOP
        IF v <= 0 THEN
            RETURN NULL;
        END IF;
        sumlog := sumlog + ln(v);
    END LOOP;
    RETURN exp(sumlog / n);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE geometric_mean(double precision) (
    STYPE      double precision[],
    SFUNC      array_append,
    FINALFUNC  geometric_mean_final,
    INITCOND   '{}'
);


CREATE OR REPLACE FUNCTION product_trans(state numeric, val numeric)
RETURNS numeric AS
$$
BEGIN
    IF val IS NOT NULL THEN
        RETURN COALESCE(state, 1) * val;
    ELSE
        RETURN state;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION product_final(state numeric)
RETURNS numeric AS
$$
BEGIN
    RETURN state;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE product(numeric) (
    STYPE     numeric,
    SFUNC     product_trans,
    FINALFUNC product_final,
    INITCOND  '1'
);


CREATE OR REPLACE FUNCTION edit_distance(s1 text, s2 text)
RETURNS integer AS
$$
DECLARE
    len1 integer;
    len2 integer;
    i integer;
    j integer;
    cost integer;
    prev integer[];
    curr integer[];
BEGIN
    IF s1 IS NULL OR s2 IS NULL THEN
        RETURN NULL;
    END IF;

    len1 := length(s1);
    len2 := length(s2);

    IF len1 = 0 THEN
        RETURN len2;
    ELSIF len2 = 0 THEN
        RETURN len1;
    END IF;

    prev := ARRAY[0];
    FOR j IN 1..len2 LOOP
        prev := prev || j;
    END LOOP;

    FOR i IN 1..len1 LOOP
        curr := ARRAY[i];
        FOR j IN 1..len2 LOOP
            IF substr(s1, i, 1) = substr(s2, j, 1) THEN
                cost := 0;
            ELSE
                cost := 1;
            END IF;

            curr := curr || LEAST(
                curr[j] + 1,
                prev[j + 1] + 1,
                prev[j] + cost
            );
        END LOOP;
        prev := curr;
    END LOOP;

    RETURN prev[len2 + 1];
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION edit_distance_similarity(s1 text, s2 text)
RETURNS integer AS
$$
DECLARE
    dist integer;
    maxlen integer;
BEGIN
    IF s1 IS NULL OR s2 IS NULL THEN
        RETURN NULL;
    END IF;
    dist := edit_distance(s1, s2);
    maxlen := greatest(length(s1), length(s2));
    IF maxlen = 0 THEN
        RETURN 100;
    END IF;
    RETURN round((1 - (dist::double precision / maxlen)) * 100);
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION checksum_trans(state bigint, val bytea)
RETURNS bigint AS
$$
BEGIN
    IF val IS NOT NULL THEN
        RETURN COALESCE(state, 0) # get_byte(digest(val, 'sha256'),0)::bigint;
    ELSE
        RETURN state;
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION checksum_final(state bigint)
RETURNS bigint AS
$$
BEGIN
    RETURN state;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE AGGREGATE checksum(bytea) (
    STYPE     bigint,
    SFUNC     checksum_trans,
    FINALFUNC checksum_final,
    INITCOND  '0'
);
