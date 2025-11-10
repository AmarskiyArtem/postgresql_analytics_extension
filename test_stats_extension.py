import hashlib
import math
import unittest
from collections import Counter
from typing import List, Sequence, Tuple


SAMPLE_ROWS: List[Tuple[str, str, float, float, bool, str]] = [
    ("A", "North", 10.5, 1.0, True, "bronze"),
    ("A", "North", 13.2, 2.0, True, "bronze"),
    ("A", "South", 15.0, 1.5, False, "silver"),
    ("B", "South", 11.0, 0.8, True, "gold"),
    ("B", "East", 18.4, 2.5, True, "gold"),
    ("B", "East", 21.0, 3.0, False, "gold"),
    ("C", "West", 9.5, 1.3, True, "silver"),
    ("C", "West", 9.5, 1.1, True, "silver"),
    ("C", "North", 14.7, 1.4, True, "bronze"),
    ("C", "South", 17.2, 2.2, True, "gold"),
    ("D", "West", 0.0, 0.5, True, "silver"),
    ("D", "East", -4.0, 0.7, True, "bronze"),
]

EXPECTED_FLOATS = {
    "skewness_pop": -0.879838531662495,
    "skewness_samp": -1.0108575127946473,
    "kurtosis_pop": 0.04522352732198387,
    "kurtosis_samp": 0.8051884934115963,
    "median": 12.1,
    "mode": 9.5,
    "entropy": 1.584962500721156,
    "avg_weighted": 14.201111111111109,
    "avg_if": 10.0,
    "geometric_mean_positive": 13.503853832517134,
    "product": 0.0,
    "edit_distance": 4,
    "edit_distance_similarity": 0.5555555555555556,
}

def _values() -> List[float]:
    return [row[2] for row in SAMPLE_ROWS]


def _weights() -> List[float]:
    return [row[3] for row in SAMPLE_ROWS]


def _flags() -> List[bool]:
    return [row[4] for row in SAMPLE_ROWS]


def _labels() -> List[str]:
    return [row[5] for row in SAMPLE_ROWS]


def population_moments(values: Sequence[float]) -> Tuple[float, float, float, float]:
    n = len(values)
    mean = sum(values) / n
    centered = [v - mean for v in values]
    m2 = sum(c ** 2 for c in centered) / n
    m3 = sum(c ** 3 for c in centered) / n
    m4 = sum(c ** 4 for c in centered) / n
    return mean, m2, m3, m4


def skewness_pop(values: Sequence[float]) -> float:
    _, m2, m3, _ = population_moments(values)
    return m3 / (m2 ** 1.5)


def skewness_samp(values: Sequence[float]) -> float:
    n = len(values)
    mean = sum(values) / n
    centered = [v - mean for v in values]
    sum_sq = sum(c ** 2 for c in centered)
    sum_cu = sum(c ** 3 for c in centered)
    s = math.sqrt(sum_sq / (n - 1))
    return (n * sum_cu) / ((n - 1) * (n - 2) * (s ** 3))


def kurtosis_pop(values: Sequence[float]) -> float:
    _, m2, _, m4 = population_moments(values)
    return m4 / (m2 * m2) - 3.0


def kurtosis_samp(values: Sequence[float]) -> float:
    n = len(values)
    mean = sum(values) / n
    centered = [v - mean for v in values]
    sum_sq = sum(c ** 2 for c in centered)
    sum_qu = sum(c ** 4 for c in centered)
    s2 = sum_sq / (n - 1)
    num = n * (n + 1) * sum_qu
    den = (n - 1) * (n - 2) * (n - 3) * (s2 ** 2)
    correction = 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
    return num / den - correction


def median(values: Sequence[float]) -> float:
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2 if n % 2 == 0 else sorted_vals[mid]


def mode(values: Sequence[float]) -> float:
    counts = Counter(values)
    max_freq = max(counts.values())
    return min(value for value, freq in counts.items() if freq == max_freq)


def entropy(labels: Sequence[str]) -> float:
    counts = Counter(labels)
    total = len(labels)
    return -sum(
        (count / total) * (math.log(count / total) / math.log(2))
        for count in counts.values()
    )


def avg_weighted(values: Sequence[float], weights: Sequence[float]) -> float:
    return sum(v * w for v, w in zip(values, weights)) / sum(weights)


def avg_if(values: Sequence[float], flags: Sequence[bool]) -> float:
    filtered = [v for v, flag in zip(values, flags) if flag]
    return sum(filtered) / len(filtered)


def geometric_mean_positive(values: Sequence[float]) -> float:
    positives = [v for v in values if v > 0]
    return math.exp(sum(math.log(v) for v in positives) / len(positives))


def product(values: Sequence[float]) -> float:
    result = 1.0
    for value in values:
        result *= value
    return result


def edit_distance(left: str, right: str) -> int:
    len_left, len_right = len(left), len(right)
    if len_left == 0:
        return len_right
    if len_right == 0:
        return len_left
    previous = list(range(len_right + 1))
    current = [0] * (len_right + 1)
    for i in range(1, len_left + 1):
        current[0] = i
        for j in range(1, len_right + 1):
            cost = 0 if left[i - 1] == right[j - 1] else 1
            current[j] = min(
                current[j - 1] + 1,
                previous[j] + 1,
                previous[j - 1] + cost,
            )
        previous, current = current, [0] * (len_right + 1)
    return previous[len_right]


def edit_distance_similarity(left: str, right: str) -> float:
    distance = edit_distance(left, right)
    max_len = max(len(left), len(right))
    return 1.0 - distance / max_len if max_len else 1.0


class StatsExtensionPurePythonTests(unittest.TestCase):
    def setUp(self):
        self.values = _values()
        self.weights = _weights()
        self.flags = _flags()
        self.labels = _labels()

    def test_skewness_and_kurtosis(self):
        self.assertAlmostEqual(skewness_pop(self.values), EXPECTED_FLOATS["skewness_pop"])
        self.assertAlmostEqual(
            skewness_samp(self.values), EXPECTED_FLOATS["skewness_samp"]
        )
        self.assertAlmostEqual(kurtosis_pop(self.values), EXPECTED_FLOATS["kurtosis_pop"])
        self.assertAlmostEqual(
            kurtosis_samp(self.values), EXPECTED_FLOATS["kurtosis_samp"]
        )

    def test_median_mode_entropy(self):
        self.assertAlmostEqual(median(self.values), EXPECTED_FLOATS["median"])
        self.assertAlmostEqual(mode(self.values), EXPECTED_FLOATS["mode"])
        self.assertAlmostEqual(entropy(self.labels), EXPECTED_FLOATS["entropy"])

    def test_weighted_and_conditional_avg(self):
        self.assertAlmostEqual(
            avg_weighted(self.values, self.weights), EXPECTED_FLOATS["avg_weighted"]
        )
        self.assertAlmostEqual(avg_if(self.values, self.flags), EXPECTED_FLOATS["avg_if"])

    def test_geometric_mean_and_product(self):
        self.assertAlmostEqual(
            geometric_mean_positive(self.values),
            EXPECTED_FLOATS["geometric_mean_positive"],
        )
        self.assertAlmostEqual(product(self.values), EXPECTED_FLOATS["product"])

    def test_edit_distance_functions(self):
        distance = edit_distance("analytics", "statics")
        similarity = edit_distance_similarity("analytics", "statics")
        self.assertEqual(distance, EXPECTED_FLOATS["edit_distance"])
        self.assertAlmostEqual(
            similarity, EXPECTED_FLOATS["edit_distance_similarity"]
        )


if __name__ == "__main__":
    unittest.main()
