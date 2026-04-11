from __future__ import annotations

from datetime import date
from decimal import Decimal
import re
import unicodedata

from .models import Market, MatchedMarkets


STOPWORDS = {"will", "the", "a", "an", "by", "on", "in", "for", "be", "is", "above", "below"}
ALIASES = {
    "bitcoin": "btc",
    "minutes": "min",
    "minute": "min",
    "over": "above",
    "under": "below",
}


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = normalized.replace("$", "")
    normalized = re.sub(r"15\s*minutes?", "15min", normalized)
    normalized = re.sub(r"15\s*m", "15min", normalized)
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def tokenize(value: str) -> set[str]:
    tokens: set[str] = set()
    for token in normalize_text(value).split():
        canonical = ALIASES.get(token, token)
        if canonical not in STOPWORDS:
            tokens.add(canonical)
    return tokens


def jaccard_similarity(left: str, right: str) -> Decimal:
    left_tokens = tokenize(left)
    right_tokens = tokenize(right)
    if not left_tokens or not right_tokens:
        return Decimal("0")
    shared = len(left_tokens & right_tokens)
    total = len(left_tokens | right_tokens)
    return Decimal(shared) / Decimal(total)


def extract_numeric_tokens(value: str) -> set[str]:
    return set(re.findall(r"\d+(?:\.\d+)?", normalize_text(value)))


def extract_direction(value: str) -> str | None:
    normalized = normalize_text(value)
    if any(token in normalized.split() for token in ("above", "over", "exceed", "higher")):
        return "above"
    if any(token in normalized.split() for token in ("below", "under", "less", "lower")):
        return "below"
    return None


def market_similarity(left: str, right: str) -> Decimal:
    token_score = jaccard_similarity(left, right)
    left_numbers = extract_numeric_tokens(left)
    right_numbers = extract_numeric_tokens(right)
    if left_numbers or right_numbers:
        number_score = Decimal(len(left_numbers & right_numbers)) / Decimal(len(left_numbers | right_numbers))
    else:
        number_score = Decimal("0.5")

    left_direction = extract_direction(left)
    right_direction = extract_direction(right)
    direction_score = Decimal("1.0") if left_direction == right_direction and left_direction is not None else Decimal("0.0")
    return (token_score * Decimal("0.5")) + (number_score * Decimal("0.35")) + (direction_score * Decimal("0.15"))


def similarity_key(question: str, end_date: date) -> str:
    return f"{normalize_text(question)}|{end_date.isoformat()}"


def _time_distance_minutes(poly: Market, kalshi: Market) -> int:
    if poly.end_time is None or kalshi.end_time is None:
        return abs((kalshi.end_date - poly.end_date).days) * 24 * 60
    return int(abs((kalshi.end_time - poly.end_time).total_seconds()) // 60)


def match_markets(
    polymarket_markets: list[Market],
    kalshi_markets: list[Market],
    max_date_drift_days: int,
    max_time_drift_minutes: int,
    min_similarity_score: Decimal,
) -> list[MatchedMarkets]:
    remaining_kalshi = list(kalshi_markets)
    matches: list[MatchedMarkets] = []

    for poly in polymarket_markets:
        scored_candidates: list[tuple[Decimal, int, Market]] = []
        for kalshi in remaining_kalshi:
            similarity = market_similarity(poly.question, kalshi.question)
            if similarity < min_similarity_score:
                continue

            if poly.end_time is not None and kalshi.end_time is not None:
                time_drift = _time_distance_minutes(poly, kalshi)
                if time_drift > max_time_drift_minutes:
                    continue
            elif abs((kalshi.end_date - poly.end_date).days) > max_date_drift_days:
                continue
            else:
                time_drift = _time_distance_minutes(poly, kalshi)

            scored_candidates.append((similarity, time_drift, kalshi))

        if not scored_candidates:
            continue

        best_similarity, _, best_market = max(scored_candidates, key=lambda item: (item[0], -item[1]))
        remaining_kalshi.remove(best_market)
        matches.append(
            MatchedMarkets(
                polymarket=poly,
                kalshi=best_market,
                similarity_key=similarity_key(poly.question, poly.end_date),
                similarity_score=best_similarity,
            )
        )
    return matches
