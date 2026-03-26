import sqlite3
import json
from flask import Flask, request, jsonify, render_template, g

DB_PATH = "fragrances.db"
app = Flask(__name__)

VOTE_GROUPS = {
    "longevity": [
        "longevity_very_weak", "longevity_weak", "longevity_moderate",
        "longevity_long_lasting", "longevity_eternal",
    ],
    "sillage": [
        "sillage_intimate", "sillage_moderate", "sillage_strong", "sillage_enormous",
    ],
    "season": [
        "season_spring", "season_summer", "season_fall", "season_winter",
    ],
    "gender": [
        "gender_female", "gender_more_female", "gender_unisex",
        "gender_more_male", "gender_male",
    ],
}
VALID_COLS = {col for cols in VOTE_GROUPS.values() for col in cols}

SORT_MAP = {
    "bayesian":        "bayesian_score",
    "rating":          "rating",
    "votes":           "votes",
    "loved":           "most_loved_score",
    "controversial":   "controversial_score",
    "price_value":     "price_value_score",
    "love_per_dollar": "love_per_dollar_score",
    "year":            "year",
    "name":            "name",
}

_GLOBALS: dict = {}


def _compute_globals() -> dict:
    conn = sqlite3.connect(DB_PATH)
    C = conn.execute(
        "SELECT AVG(rating) FROM fragrances WHERE rating IS NOT NULL AND votes IS NOT NULL"
    ).fetchone()[0] or 3.99

    n = conn.execute(
        "SELECT COUNT(*) FROM fragrances WHERE votes IS NOT NULL"
    ).fetchone()[0]
    mid = (n - 1) // 2
    rows = conn.execute(
        f"SELECT votes FROM fragrances WHERE votes IS NOT NULL ORDER BY votes LIMIT 2 OFFSET {mid}"
    ).fetchall()
    if n % 2 == 1:
        m = rows[0][0]
    else:
        m = (rows[0][0] + rows[1][0]) / 2 if len(rows) >= 2 else rows[0][0]

    # Price value Bayesian params
    price_rows = conn.execute("""
        SELECT
            (1.0*price_way_overpriced + 2*price_overpriced + 3*price_ok + 4*price_good_value + 5*price_great_value)
            / (price_way_overpriced+price_overpriced+price_ok+price_good_value+price_great_value) AS raw_val,
            (price_way_overpriced+price_overpriced+price_ok+price_good_value+price_great_value) AS price_total
        FROM fragrances
        WHERE (price_way_overpriced+price_overpriced+price_ok+price_good_value+price_great_value) > 0
        ORDER BY price_total
    """).fetchall()
    C_price = sum(r[0] for r in price_rows) / len(price_rows) if price_rows else 3.0
    pn = len(price_rows)
    pmid = (pn - 1) // 2
    if pn % 2 == 1:
        m_price = price_rows[pmid][1]
    else:
        m_price = (price_rows[pmid][1] + price_rows[pmid + 1][1]) / 2 if pn >= 2 else price_rows[0][1]

    # Note usage stats
    note_rows = conn.execute("""
        WITH all_notes AS (
            SELECT json_extract(value,'$.name') AS name, 'top' AS layer
            FROM fragrances, json_each(top_notes_json) WHERE top_notes_json IS NOT NULL
            UNION ALL
            SELECT json_extract(value,'$.name'), 'mid'
            FROM fragrances, json_each(middle_notes_json) WHERE middle_notes_json IS NOT NULL
            UNION ALL
            SELECT json_extract(value,'$.name'), 'base'
            FROM fragrances, json_each(base_notes_json) WHERE base_notes_json IS NOT NULL
        )
        SELECT name,
            COUNT(*) AS total,
            SUM(CASE WHEN layer='top'  THEN 1 ELSE 0 END) AS top_count,
            SUM(CASE WHEN layer='mid'  THEN 1 ELSE 0 END) AS mid_count,
            SUM(CASE WHEN layer='base' THEN 1 ELSE 0 END) AS base_count
        FROM all_notes WHERE name IS NOT NULL
        GROUP BY name ORDER BY total DESC
    """).fetchall()
    note_images = {r[0]: r[1] for r in conn.execute(
        "SELECT name, image_url FROM notes WHERE image_url IS NOT NULL"
    ).fetchall()}
    note_stats = [
        {"name": r[0], "total": r[1], "top": r[2], "mid": r[3], "base": r[4],
         "image_url": note_images.get(r[0])}
        for r in note_rows
    ]

    # Accord usage stats
    accord_rows = conn.execute("""
        SELECT acc_name, COUNT(*) AS count, AVG(acc_strength) AS avg_strength
        FROM (
            SELECT json_extract(value,'$.name')         AS acc_name,
                   json_extract(value,'$.strength_pct') AS acc_strength
            FROM fragrances, json_each(accords_json)
            WHERE accords_json IS NOT NULL
        )
        WHERE acc_name IS NOT NULL
        GROUP BY acc_name ORDER BY count DESC
    """).fetchall()
    accord_stats = [
        {"name": r[0], "count": r[1],
         "avg_strength": round(r[2], 1) if r[2] is not None else None}
        for r in accord_rows
    ]

    total = conn.execute("SELECT COUNT(*) FROM fragrances").fetchone()[0]
    brands = [r[0] for r in conn.execute(
        "SELECT DISTINCT brand FROM fragrances WHERE brand IS NOT NULL ORDER BY brand"
    ).fetchall()]
    conn.close()
    return {
        "mean_rating": round(C, 4), "median_votes": m, "total_count": total, "brand_list": brands,
        "mean_price_value": round(C_price, 4), "median_price_votes": m_price,
        "note_stats": note_stats, "accord_stats": accord_stats,
    }


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _build_condition_sql(cond: dict) -> tuple[str, list] | None:
    """Return (sql_fragment, params) for one note/accord condition, or None if invalid."""
    ctype = cond.get("type", "")
    name = cond.get("name", "").strip()
    if not name:
        return None

    min_pct = cond.get("min_pct")
    max_pct = cond.get("max_pct")
    # upper_only means "absent or below threshold" semantics
    upper_only = max_pct is not None and min_pct is None
    name_pattern = f"%{name}%"

    def strength_clauses(min_p, max_p):
        clauses, p = [], []
        if min_p is not None and max_p is not None:
            clauses.append("json_extract(value,'$.strength_pct') BETWEEN ? AND ?")
            p += [float(min_p), float(max_p)]
        elif min_p is not None:
            clauses.append("json_extract(value,'$.strength_pct') >= ?")
            p.append(float(min_p))
        elif max_p is not None:
            clauses.append("json_extract(value,'$.strength_pct') <= ?")
            p.append(float(max_p))
        return clauses, p

    def exists_in(json_col):
        s_clauses, s_params = strength_clauses(min_pct, max_pct)
        where = " AND ".join(["lower(json_extract(value,'$.name')) LIKE ?"] + s_clauses)
        return f"SELECT 1 FROM json_each({json_col}) WHERE {where}", [name_pattern] + s_params

    if ctype == "accord":
        if upper_only:
            absent = f"SELECT 1 FROM json_each(accords_json) WHERE lower(json_extract(value,'$.name')) LIKE ?"
            e_sql, e_p = exists_in("accords_json")
            return f"(NOT EXISTS ({absent}) OR EXISTS ({e_sql}))", [name_pattern] + e_p
        e_sql, e_p = exists_in("accords_json")
        return f"EXISTS ({e_sql})", e_p

    note_cols_map = {
        "top":      ["top_notes_json"],
        "mid":      ["middle_notes_json"],
        "base":     ["base_notes_json"],
        "any_note": ["top_notes_json", "middle_notes_json", "base_notes_json"],
    }
    cols = note_cols_map.get(ctype)
    if not cols:
        return None

    if upper_only:
        absent_parts = [
            f"NOT EXISTS (SELECT 1 FROM json_each({col}) WHERE lower(json_extract(value,'$.name')) LIKE ?)"
            for col in cols
        ]
        absent_params = [name_pattern] * len(cols)
        exists_parts, exists_params = [], []
        for col in cols:
            e_sql, e_p = exists_in(col)
            exists_parts.append(f"EXISTS ({e_sql})")
            exists_params += e_p
        sql = f"(({' AND '.join(absent_parts)}) OR ({' OR '.join(exists_parts)}))"
        return sql, absent_params + exists_params

    parts, all_params = [], []
    for col in cols:
        e_sql, e_p = exists_in(col)
        parts.append(f"EXISTS ({e_sql})")
        all_params += e_p
    return "(" + " OR ".join(parts) + ")", all_params


def build_query(args: dict):
    C = _GLOBALS["mean_rating"]
    m = _GLOBALS["median_votes"]
    C_price = _GLOBALS["mean_price_value"]
    m_price = _GLOBALS["median_price_votes"]

    # Sentiment score expressions
    total_sent = "(COALESCE(rating_love,0)+COALESCE(rating_like,0)+COALESCE(rating_ok,0)+COALESCE(rating_dislike,0)+COALESCE(rating_hate,0))"
    # Most Loved: Bayesian net sentiment — love>like, hate worse than dislike, smoothed by m phantom votes
    most_loved_expr = (
        f"(2.0*COALESCE(rating_love,0) + COALESCE(rating_like,0)"
        f" - COALESCE(rating_dislike,0) - 2.0*COALESCE(rating_hate,0))"
        f" / NULLIF({total_sent} + {m}, 0)"
    )
    # Friendly: kept for love_per_dollar denominator (positive rate, Bayesian-smoothed)
    friendly_expr = f"CAST(COALESCE(rating_love,0)+COALESCE(rating_like,0) AS REAL) / NULLIF({total_sent}+{m},0)"
    # Controversial: 2 * min(P(liked), P(disliked)) — peaks at 1.0 when 50/50 split
    controversial_expr = (
        f"2.0 * MIN("
        f"  CAST(COALESCE(rating_love,0)+COALESCE(rating_like,0) AS REAL) / NULLIF({total_sent},0),"
        f"  CAST(COALESCE(rating_dislike,0)+COALESCE(rating_hate,0) AS REAL) / NULLIF({total_sent},0)"
        f")"
    )

    _pt = "(COALESCE(price_way_overpriced,0)+COALESCE(price_overpriced,0)+COALESCE(price_ok,0)+COALESCE(price_good_value,0)+COALESCE(price_great_value,0))"
    _praw = f"(1.0*COALESCE(price_way_overpriced,0)+2*COALESCE(price_overpriced,0)+3*COALESCE(price_ok,0)+4*COALESCE(price_good_value,0)+5*COALESCE(price_great_value,0)) / NULLIF({_pt},0)"
    price_value_expr = f"({C_price} * {m_price} + ({_praw}) * {_pt}) / ({m_price} + {_pt})"
    love_per_dollar_expr = f"({friendly_expr}) / NULLIF(6.0 - ({price_value_expr}), 0)"

    select = f"""
        SELECT
            id, name, brand, year, url, rating, votes, image_url,
            ({C} * {m} + COALESCE(rating,0) * COALESCE(votes,0)) / ({m} + COALESCE(votes,0)) AS bayesian_score,
            {price_value_expr} AS price_value_score,
            {love_per_dollar_expr} AS love_per_dollar_score,
            {most_loved_expr} AS most_loved_score,
            {controversial_expr} AS controversial_score,
            longevity_very_weak, longevity_weak, longevity_moderate, longevity_long_lasting, longevity_eternal,
            sillage_intimate, sillage_moderate, sillage_strong, sillage_enormous,
            season_spring, season_summer, season_fall, season_winter,
            gender_female, gender_more_female, gender_unisex, gender_more_male, gender_male,
            rating_love, rating_like, rating_ok, rating_dislike, rating_hate,
            time_day, time_night,
            price_way_overpriced, price_overpriced, price_ok, price_good_value, price_great_value
        FROM fragrances
    """

    wheres, params = [], []

    q = args.get("q", "").strip()
    if q:
        wheres.append("(name LIKE ? OR brand LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]

    brand = args.get("brand", "").strip()
    if brand:
        wheres.append("brand = ?")
        params.append(brand)

    for key, col_name in [("year_min", "year >="), ("year_max", "year <=")]:
        try:
            val = int(args.get(key, ""))
            op = col_name.split()[1]
            field = col_name.split()[0]
            wheres.append(f"{field} {op} ?")
            params.append(val)
        except (ValueError, TypeError):
            pass

    for key, expr in [("rating_min", "rating >= ?"), ("rating_max", "rating <= ?")]:
        try:
            val = float(args.get(key, ""))
            wheres.append(expr)
            params.append(val)
        except (ValueError, TypeError):
            pass

    try:
        votes_min = int(args.get("votes_min", ""))
        wheres.append("votes >= ?")
        params.append(votes_min)
    except (ValueError, TypeError):
        pass

    _gender_param = args.get("gender", "").strip()
    _s = lambda c: f"COALESCE({c},0)"
    if _gender_param == "male_wearable":
        wheres.append(
            f"{_s('gender_unisex')}+{_s('gender_more_male')}+{_s('gender_male')}"
            f" > {_s('gender_female')}+{_s('gender_more_female')}"
        )
    elif _gender_param == "female_wearable":
        wheres.append(
            f"{_s('gender_female')}+{_s('gender_more_female')}+{_s('gender_unisex')}"
            f" > {_s('gender_male')}+{_s('gender_more_male')}"
        )

    _season_param = args.get("season", "").strip()
    _season_total = f"NULLIF({_s('season_spring')}+{_s('season_summer')}+{_s('season_fall')}+{_s('season_winter')},0)"
    _time_total   = f"NULLIF({_s('time_day')}+{_s('time_night')},0)"
    if _season_param == "hot":
        wheres.append(f"{_s('season_spring')}+{_s('season_summer')} > {_s('season_fall')}+{_s('season_winter')}")
    elif _season_param == "cold":
        wheres.append(f"{_s('season_fall')}+{_s('season_winter')} > {_s('season_spring')}+{_s('season_summer')}")
    elif _season_param == "universal":
        wheres.append(
            f"CAST({_s('season_spring')} AS REAL)/{_season_total} >= 0.20"
            f" AND CAST({_s('season_summer')} AS REAL)/{_season_total} >= 0.20"
            f" AND CAST({_s('season_fall')}   AS REAL)/{_season_total} >= 0.20"
            f" AND CAST({_s('season_winter')} AS REAL)/{_season_total} >= 0.20"
            f" AND CAST({_s('time_day')}   AS REAL)/{_time_total} >= 0.40"
            f" AND CAST({_s('time_night')} AS REAL)/{_time_total} >= 0.40"
        )

    for group_name, cols in VOTE_GROUPS.items():
        selected = args.get(group_name, "").strip()
        if not selected:
            continue
        if group_name == "season" and _season_param in ("hot", "cold", "universal"):
            continue
        if group_name == "gender" and _gender_param in ("male_wearable", "female_wearable"):
            continue
        full_col = f"{group_name}_{selected}"
        if full_col not in VALID_COLS:
            continue
        coalesced = [f"COALESCE({c},0)" for c in cols]
        wheres.append(f"COALESCE({full_col},0) = MAX({', '.join(coalesced)}) AND {full_col} > 0")

    conditions_raw = args.get("conditions", "")
    if conditions_raw:
        try:
            conditions = json.loads(conditions_raw)
        except Exception:
            conditions = []
        for cond in conditions:
            result = _build_condition_sql(cond)
            if result:
                sql_frag, cond_params = result
                wheres.append(sql_frag)
                params += cond_params

    where_clause = ("WHERE " + " AND ".join(wheres)) if wheres else ""

    sort_col = SORT_MAP.get(args.get("sort", "bayesian"), "bayesian_score")
    order = "ASC" if args.get("order", "desc") == "asc" else "DESC"

    try:
        page = max(1, int(args.get("page", 1)))
    except (ValueError, TypeError):
        page = 1
    try:
        page_size = min(100, max(10, int(args.get("page_size", 25))))
    except (ValueError, TypeError):
        page_size = 25
    offset = (page - 1) * page_size

    count_sql = f"SELECT COUNT(*) FROM fragrances {where_clause}"
    data_sql = f"{select} {where_clause} ORDER BY {sort_col} {order} NULLS LAST LIMIT {page_size} OFFSET {offset}"
    return count_sql, data_sql, params, page, page_size


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/ingredient-stats")
def ingredient_stats():
    return jsonify({
        "notes": _GLOBALS.get("note_stats", []),
        "accords": _GLOBALS.get("accord_stats", []),
    })


@app.route("/api/notes")
def notes():
    db = get_db()
    rows = db.execute("SELECT name, image_url FROM notes WHERE image_url IS NOT NULL").fetchall()
    return jsonify({r["name"]: r["image_url"] for r in rows})


@app.route("/api/stats")
def stats():
    return jsonify(_GLOBALS)


@app.route("/api/search")
def search():
    count_sql, data_sql, params, page, page_size = build_query(request.args)
    db = get_db()
    total = db.execute(count_sql, params).fetchone()[0]
    rows = db.execute(data_sql, params).fetchall()
    results = [dict(r) for r in rows]
    pages = max(1, (total + page_size - 1) // page_size)
    return jsonify({"total": total, "page": page, "page_size": page_size, "pages": pages, "results": results})


@app.route("/api/fragrance/<int:frag_id>")
def get_fragrance(frag_id):
    db = get_db()
    row = db.execute("SELECT * FROM fragrances WHERE id = ?", (frag_id,)).fetchone()
    if not row:
        return jsonify({"error": "not found"}), 404
    data = dict(row)
    for col in ("top_notes_json", "middle_notes_json", "base_notes_json", "accords_json"):
        val = data.get(col)
        if isinstance(val, str):
            try:
                data[col] = json.loads(val)
            except Exception:
                data[col] = []
    return jsonify(data)


if __name__ == "__main__":
    _GLOBALS.update(_compute_globals())
    print(f"Globals: mean_rating={_GLOBALS['mean_rating']}, median_votes={_GLOBALS['median_votes']}, total={_GLOBALS['total_count']}")
    app.run(debug=True, port=5000, host='0.0.0.0')
