def _escape_sql_literal(s: str) -> str:
    return s.replace("'", "''")

def _in_list_sql(values: list[str]) -> str:
    escaped = [f"'{_escape_sql_literal(v)}'" for v in values if v]
    return "(" + ",".join(escaped) + ")" if escaped else "()"

def _violence_where(violence_filter: str) -> str:
    if violence_filter == "Violent only":
        return "AND is_violent = true"
    if violence_filter == "Non-violent only":
        return "AND is_violent = false"
    return ""

def _type_where(crime_types: list[str]) -> str:
    if not crime_types:
        return ""
    return f"AND crime_description IN {_in_list_sql(crime_types)}"


def q_date_bounds_daily_area() -> str:
    return """
    SELECT
      MIN(occurrence_date) AS min_date,
      MAX(occurrence_date) AS max_date
    FROM lapd.gold.mart_crime_daily_area
    """

def q_map_agg_daily_area(date_from: str, date_to: str) -> str:
    return f"""
    SELECT
      area_id,
      area_name,
      SUM(crime_count) AS crime_count,
      SUM(violent_count) AS violent_count,
      CASE WHEN SUM(crime_count) = 0 THEN 0
           ELSE SUM(violent_count) * 1.0 / SUM(crime_count)
      END AS violent_share,
      AVG(avg_reporting_delay_days) AS avg_reporting_delay_days,
      AVG(median_reporting_delay_days) AS median_reporting_delay_days
    FROM lapd.gold.mart_crime_daily_area
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
    GROUP BY area_id, area_name
    """

def q_top_crime_descriptions(date_from: str, date_to: str, violence_filter: str, limit: int = 60) -> str:
    vw = _violence_where(violence_filter)
    return f"""
    SELECT
      crime_description,
      COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {vw}
      AND crime_description IS NOT NULL
    GROUP BY crime_description
    ORDER BY incidents DESC
    LIMIT {int(limit)}
    """

def q_map_agg_fact_area(date_from: str, date_to: str, violence_filter: str, crime_types: list[str]) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    return f"""
    SELECT
      area_id,
      area_name,
      COUNT(*) AS crime_count,
      SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_count,
      CASE WHEN COUNT(*) = 0 THEN 0
           ELSE SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
      END AS violent_share,
      AVG(reporting_delay_days) AS avg_reporting_delay_days,
      percentile_approx(reporting_delay_days, 0.5) AS median_reporting_delay_days
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {vw}
      {tw}
    GROUP BY area_id, area_name
    """

def q_daily_series_fact_for_area(date_from: str, date_to: str, area_id: str, violence_filter: str, crime_types: list[str]) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    area_id_esc = _escape_sql_literal(area_id)
    return f"""
    SELECT
      occurrence_date,
      COUNT(*) AS crime_count,
      SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_count,
      CASE WHEN COUNT(*) = 0 THEN 0
           ELSE SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
      END AS violent_share
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      AND area_id = '{area_id_esc}'
      {vw}
      {tw}
    GROUP BY occurrence_date
    ORDER BY occurrence_date
    """

def q_top_descriptions_for_area(date_from: str, date_to: str, area_id: str, violence_filter: str, crime_types: list[str], limit: int = 12) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    area_id_esc = _escape_sql_literal(area_id)
    return f"""
    SELECT
      crime_description,
      COUNT(*) AS incidents,
      SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      AND area_id = '{area_id_esc}'
      {vw}
      {tw}
      AND crime_description IS NOT NULL
    GROUP BY crime_description
    ORDER BY incidents DESC
    LIMIT {int(limit)}
    """

def q_monthly_trends_fact(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      date_trunc('month', occurrence_ts) AS month_start,
      crime_description,
      COUNT(*) AS incidents,
      SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw}
      {vw}
      {tw}
      AND crime_description IS NOT NULL
    GROUP BY date_trunc('month', occurrence_ts), crime_description
    ORDER BY month_start, crime_description
    """

def q_hourly_weekday_fact(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      weekday,
      hour_occ,
      COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw}
      {vw}
      {tw}
    GROUP BY weekday, hour_occ
    """

def q_area_list() -> str:
    return """
    SELECT DISTINCT area_id, area_name
    FROM lapd.gold.mart_crime_daily_area
    WHERE area_id IS NOT NULL AND area_name IS NOT NULL
    ORDER BY area_name
    """

def q_monthly_total_fact(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      date_trunc('month', occurrence_ts) AS month_start,
      COUNT(*) AS incidents,
      SUM(CASE WHEN is_violent THEN 1 ELSE 0 END) AS violent_incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw}
      {vw}
      {tw}
    GROUP BY date_trunc('month', occurrence_ts)
    ORDER BY month_start
    """
    
def q_total_incidents_fact(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw}
      {vw}
      {tw}
    """

def q_dist_victim_sex(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      COALESCE(NULLIF(TRIM(victim_sex), ''), 'Unknown') AS victim_sex,
      COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw} {vw} {tw}
    GROUP BY COALESCE(NULLIF(TRIM(victim_sex), ''), 'Unknown')
    ORDER BY incidents DESC
    """

def q_dist_victim_age_group(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      COALESCE(NULLIF(TRIM(victim_age_group), ''), 'Unknown') AS victim_age_group,
      COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw} {vw} {tw}
    GROUP BY COALESCE(NULLIF(TRIM(victim_age_group), ''), 'Unknown')
    ORDER BY incidents DESC
    """

def q_dist_victim_descent(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None, limit: int = 20) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      COALESCE(NULLIF(TRIM(victim_descent), ''), 'Unknown') AS victim_descent,
      COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw} {vw} {tw}
    GROUP BY COALESCE(NULLIF(TRIM(victim_descent), ''), 'Unknown')
    ORDER BY incidents DESC
    LIMIT {int(limit)}
    """

def q_weapon_share(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      SUM(CASE WHEN has_weapon THEN 1 ELSE 0 END) AS with_weapon,
      COUNT(*) AS total
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw} {vw} {tw}
    """

def q_top_weapon_descriptions(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None, limit: int = 15) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      COALESCE(NULLIF(TRIM(weapon_description), ''), 'Unknown') AS weapon_description,
      COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw} {vw} {tw}
      AND has_weapon = true
    GROUP BY COALESCE(NULLIF(TRIM(weapon_description), ''), 'Unknown')
    ORDER BY incidents DESC
    LIMIT {int(limit)}
    """

def q_top_premises(date_from: str, date_to: str, violence_filter: str, crime_types: list[str], area_id: str | None = None, limit: int = 15) -> str:
    vw = _violence_where(violence_filter)
    tw = _type_where(crime_types)
    aw = f"AND area_id = '{_escape_sql_literal(area_id)}'" if area_id else ""
    return f"""
    SELECT
      COALESCE(NULLIF(TRIM(premise_description), ''), 'Unknown') AS premise_description,
      COUNT(*) AS incidents
    FROM lapd.gold.fact_crime_incidents_gold
    WHERE occurrence_date BETWEEN '{date_from}' AND '{date_to}'
      {aw} {vw} {tw}
    GROUP BY COALESCE(NULLIF(TRIM(premise_description), ''), 'Unknown')
    ORDER BY incidents DESC
    LIMIT {int(limit)}
    """
