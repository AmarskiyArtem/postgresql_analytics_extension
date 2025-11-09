-- Demo script for stats_extension showing how to prepare data
-- and how to call every function/aggregate defined in stats_extension--1.0.sql.

DROP DATABASE IF EXISTS stats_extension_demo;
CREATE DATABASE stats_extension_demo;
\connect stats_extension_demo

CREATE EXTENSION IF NOT EXISTS stats_extension;

-- ---------------------------------------------------------------------------
-- Base tables and sample data
-- ---------------------------------------------------------------------------
CREATE TABLE metric_samples (
    sample_id serial PRIMARY KEY,
    cohort text NOT NULL,
    region text NOT NULL,
    metric_value double precision NOT NULL,
    metric_value_numeric numeric(12,4) NOT NULL,
    metric_weight double precision NOT NULL,
    metric_weight_numeric numeric(12,4) NOT NULL,
    include_in_avg boolean NOT NULL,
    quality_label text NOT NULL,
    note text
);

INSERT INTO metric_samples (
    cohort,
    region,
    metric_value,
    metric_value_numeric,
    metric_weight,
    metric_weight_numeric,
    include_in_avg,
    quality_label,
    note
) VALUES
    ('A', 'North', 10.5, 10.5, 1.0, 1.0, TRUE, 'bronze', 'alpha'),
    ('A', 'North', 13.2, 13.2, 2.0, 2.0, TRUE, 'bronze', 'alpha'),
    ('A', 'South', 15.0, 15.0, 1.5, 1.5, FALSE, 'silver', 'beta'),
    ('B', 'South', 11.0, 11.0, 0.8, 0.8, TRUE, 'gold', 'beta'),
    ('B', 'East', 18.4, 18.4, 2.5, 2.5, TRUE, 'gold', 'gamma'),
    ('B', 'East', 21.0, 21.0, 3.0, 3.0, FALSE, 'gold', 'gamma'),
    ('C', 'West', 9.5, 9.5, 1.3, 1.3, TRUE, 'silver', 'delta'),
    ('C', 'West', 9.5, 9.5, 1.1, 1.1, TRUE, 'silver', 'epsilon'),
    ('C', 'North', 14.7, 14.7, 1.4, 1.4, TRUE, 'bronze', 'delta'),
    ('C', 'South', 17.2, 17.2, 2.2, 2.2, TRUE, 'gold', 'epsilon'),
    ('D', 'West', 0.0, 0.0, 0.5, 0.5, TRUE, 'silver', 'omega'),
    ('D', 'East', -4.0, -4.0, 0.7, 0.7, TRUE, 'bronze', 'omega');

TABLE metric_samples;

-- ---------------------------------------------------------------------------
-- Final function demos
-- ---------------------------------------------------------------------------

-- Shape descriptors: population/sample skewness and kurtosis
SELECT
    skewness_pop(metric_value) AS skewness_pop,
    skewness_samp(metric_value) AS skewness_samp,
    kurtosis_pop(metric_value) AS kurtosis_pop,
    kurtosis_samp(metric_value) AS kurtosis_samp
FROM metric_samples;

-- Median / mode / entropy
SELECT
    median(metric_value) AS median_value,
    mode(metric_value) AS mode_value,
    entropy(quality_label) AS entropy_quality
FROM metric_samples;

-- Weighted average
SELECT avg_weighted(metric_value, metric_weight) AS avg_weighted_value
FROM metric_samples;

-- Conditional average
SELECT avg_if(metric_value, include_in_avg) AS avg_if_value
FROM metric_samples;

-- Contingency table
SELECT contingency(cohort, quality_label) AS contingency_matrix
FROM metric_samples;

-- Geometric mean (positive values only to avoid NULL from negatives)
SELECT geometric_mean(metric_value) FILTER (WHERE metric_value > 0) AS geometric_mean_positive
FROM metric_samples;

-- Product aggregate
SELECT product(metric_value) AS product_all_values
FROM metric_samples;

-- Scalar helpers
SELECT edit_distance('analytics', 'statics') AS edit_distance_example;
SELECT edit_distance_similarity('analytics', 'statics') AS edit_distance_similarity_example;
SELECT checksum(cohort::text, region::text, quality_label) AS checksum_example
FROM metric_samples
WHERE sample_id = 1;
