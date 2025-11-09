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
    n := COALESCE(array_length(vals, 1), 0);
    IF n = 0 THEN
        RETURN NULL;
    END IF;
    SELECT avg(val), stddev_pop(val) INTO mean_value, sd
      FROM unnest(vals) AS val;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val LOOP
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
    n := COALESCE(array_length(vals, 1), 0);
    IF n < 3 THEN
        RETURN NULL;
    END IF;
    SELECT avg(val), stddev_samp(val) INTO mean_value, sd
      FROM unnest(vals) AS val;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val LOOP
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
    n := COALESCE(array_length(vals, 1), 0);
    IF n = 0 THEN
        RETURN NULL;
    END IF;
    SELECT avg(val), stddev_pop(val) INTO mean_value, sd
      FROM unnest(vals) AS val;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val LOOP
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
    n := COALESCE(array_length(vals, 1), 0);
    IF n < 4 THEN
        RETURN NULL;
    END IF;
    SELECT avg(val), stddev_samp(val) INTO mean_value, sd
      FROM unnest(vals) AS val;
    IF sd IS NULL OR sd = 0 THEN
        RETURN 0;
    END IF;
    FOR v IN SELECT val FROM unnest(vals) AS val LOOP
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
    INITCOND      '{}' :: double precision[]
);

CREATE OR REPLACE AGGREGATE skewness_samp(double precision) (
    STYPE    double precision[],
    SFUNC    array_append,
    FINALFUNC     _skewness_samp_array,
    INITCOND      '{}' :: double precision[]
);

CREATE OR REPLACE AGGREGATE kurtosis_pop(double precision) (
    STYPE    double precision[],
    SFUNC    array_append,
    FINALFUNC     _kurtosis_pop_array,
    INITCOND      '{}' :: double precision[]
);

CREATE OR REPLACE AGGREGATE kurtosis_samp(double precision) (
    STYPE    double precision[],
    SFUNC    array_append,
    FINALFUNC     _kurtosis_samp_array,
    INITCOND      '{}' :: double precision[]
);


CREATE OR REPLACE FUNCTION median_final(values anyarray)
RETURNS anyelement AS
$$
BEGIN
    RETURN (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
            FROM unnest(values) AS v);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE median(anyelement) (
    STYPE      anyarray,
    SFUNC      array_append,
    FINALFUNC  median_final,
    INITCOND   '{}'::anyarray
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
        GROUP BY v
        ORDER BY cnt DESC, val
        LIMIT 1
    ) AS sub;
    RETURN rec.val;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE mode(anyelement) (
    STYPE      anyarray,
    SFUNC      array_append,
    FINALFUNC  mode_final,
    INITCOND   '{}'::anyarray
);


CREATE OR REPLACE FUNCTION entropy_final(values anyarray)
RETURNS double precision AS
$$
DECLARE
    total integer;
    entropy double precision := 0;
    rec record;
BEGIN
    total := COALESCE(array_length(values, 1), 0);
    IF total = 0 THEN
        RETURN NULL;
    END IF;
    FOR rec IN SELECT v AS val, count(*)::double precision AS cnt
               FROM unnest(values) AS v
               GROUP BY v LOOP
        entropy := entropy - (rec.cnt/total) * ln(rec.cnt/total);
    END LOOP;
    RETURN entropy;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE entropy(anyelement) (
    STYPE      anyarray,
    SFUNC      array_append,
    FINALFUNC  entropy_final,
    INITCOND   '{}'::anyarray
);


CREATE OR REPLACE FUNCTION avg_weighted_trans(state double precision[], value double precision, weight double precision)
RETURNS double precision[] AS
$$
BEGIN
    IF weight IS NOT NULL THEN
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
    IF state[2] IS NULL OR state[2] = 0 THEN
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
    IF cond THEN
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
    IF state[2] IS NULL OR state[2] = 0 THEN
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


CREATE OR REPLACE FUNCTION contingency_final(vals1 anyarray, vals2 anyarray)
RETURNS double precision AS
$$
DECLARE
    n integer;
    chi2 double precision := 0;
    rows integer;
    cols integer;
    total double precision;
    r_counts jsonb;
    c_counts jsonb;
    rc_counts jsonb;
    r text;
    c text;
    observed double precision;
    expected double precision;
BEGIN
    n := COALESCE(array_length(vals1,1),0);
    IF n = 0 OR array_length(vals2,1) != n THEN
        RETURN NULL;
    END IF;

    r_counts := '{}'::jsonb;
    c_counts := '{}'::jsonb;
    rc_counts := '{}'::jsonb;
    FOR i IN 1..n LOOP
        r := vals1[i]::text;
        c := vals2[i]::text;
        r_counts := r_counts || jsonb_build_object(r, COALESCE((r_counts->>r)::integer,0) + 1);
        c_counts := c_counts || jsonb_build_object(c, COALESCE((c_counts->>c)::integer,0) + 1);
        rc_counts := rc_counts || jsonb_build_object(r||'|'||c, COALESCE((rc_counts->>(r||'|'||c))::integer,0) + 1);
    END LOOP;
    rows := jsonb_object_keys(r_counts)::integer;
    cols := jsonb_object_keys(c_counts)::integer;
    IF rows = 0 OR cols = 0 THEN
        RETURN NULL;
    END IF;
    total := n;
    FOR r_key IN SELECT key FROM jsonb_each_text(r_counts) LOOP
        FOR c_key IN SELECT key FROM jsonb_each_text(c_counts) LOOP
            observed := COALESCE((rc_counts->>(r_key.key || '|' || c_key.key))::double precision, 0);
            expected := ((r_counts->>r_key.key)::double precision * (c_counts->>c_key.key)::double precision) / total;
            IF expected > 0 THEN
                chi2 := chi2 + power(observed - expected,2) / expected;
            END IF;
        END LOOP;
    END LOOP;

    RETURN sqrt(chi2 / (total * (least(rows-1, cols-1))));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE AGGREGATE contingency(val1 anyelement, val2 anyelement) (
    STYPE      anyarray[],
    SFUNC      array_append,
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
    n := COALESCE(array_length(values, 1), 0);
    IF n = 0 THEN RETURN NULL; END IF;
    FOR v IN SELECT val FROM unnest(values) AS val LOOP
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
    INITCOND   '{}'::double precision[]
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

CREATE OR REPLACE AGGREGATE product(numeric) (
    STYPE     numeric,
    SFUNC     product_trans,
    FINALFUNC   COALESCE,
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
