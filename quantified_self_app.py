# %%
import streamlit as st
import pandas as pd
import altair as alt
import psycopg2
import os
import numpy as np

# Connect to database and enable running of queries
# %%
db_host = os.getenv("SWA_DB_HOST")
db_port = os.getenv("SWA_DB_PORT")
db_db = os.getenv("SWA_DB_DB")
db_user = os.getenv("SWA_DB_USER")
db_pass = os.getenv("SWA_DB_PASS")


def create_df_from_query(sql_query):
    """
    Uses stored credentials to open a connection to the database, run a provided query,
    then close the connection. Returns a data frame of results.
    """
    conn = psycopg2.connect(
        host=db_host, port=db_port, database=db_db, user=db_user, password=db_pass
    )
    cur = conn.cursor()
    df = pd.read_sql_query(sql_query, conn)
    cur.close()
    conn.close()
    return df


# Define visualization function


def graph_as_bullet_sparkline(
    pit_data=None,
    hist_data=None,
    actual_column=None,
    target_column=None,
    above_column=None,
    low_value_column=None,
    failing_value_column=None,
    description_column=None,
    flagged_description_column=None,
    time_column=None,
    filter_field=None,
    filter_value=None,
):

    bullet_chart = (
        alt.layer(
            alt.Chart()
            .mark_bar(
                color="#c0b8b4",
            )
            .encode(alt.X(f"{above_column}:Q", scale=alt.Scale(nice=False), title=None))
            .properties(height=50, width=500),
            alt.Chart().mark_bar(color="#a59c99").encode(x=f"{low_value_column}:Q"),
            alt.Chart().mark_bar(color="#8b827f").encode(x=f"{failing_value_column}:Q"),
            alt.Chart()
            .mark_bar(color="#385B9F", size=7)
            .encode(
                x=f"{actual_column}:Q",
                tooltip=[
                    alt.Tooltip(f"{target_column}:Q", title="System Target"),
                    alt.Tooltip(f"{actual_column}:Q", title="Actual"),
                    alt.Tooltip(
                        f"{failing_value_column}:Q", title="Failure State Threshold"
                    ),
                    alt.Tooltip(f"{low_value_column}:Q", title="Warning Threshold"),
                ],
            ),
            alt.Chart().mark_tick(color="black").encode(x=f"{target_column}:Q"),
            data=pit_data,
        )
        .facet(
            row=alt.Row(
                f"{flagged_description_column}:O",
                sort="ascending",
                title=None,
                header=alt.Header(labelOrient="top", labelAnchor="start"),
            )
        )
        .resolve_scale(x="independent")
        .transform_filter(
            alt.FieldOneOfPredicate(field=f"{filter_field}", oneOf=filter_value)
        )
    )

    sparkline = (
        alt.layer(
            alt.Chart()
            .mark_area(color="#c0b8b4")
            .encode(
                alt.X(
                    f"{time_column}:T",
                    scale=alt.Scale(nice=False),
                    title=None,
                    axis=alt.Axis(labels=False, grid=False, domain=False, ticks=False),
                ),
                alt.Y(
                    f"{above_column}:Q",
                    scale=alt.Scale(nice=False),
                    title=None,
                    axis=alt.Axis(labels=False, grid=False, domain=False, ticks=False),
                ),
            )
            .properties(height=50),
            alt.Chart()
            .mark_area(color="#a59c99")
            .encode(x=f"{time_column}:T", y=f"{low_value_column}:Q"),
            alt.Chart()
            .mark_area(color="#8b827f")
            .encode(x=f"{time_column}:T", y=f"{failing_value_column}:Q"),
            alt.Chart()
            .mark_line(color="#385B9F")
            .encode(x=f"{time_column}:T", y=f"{actual_column}:Q"),
            alt.Chart()
            .mark_line(color="black", size=1)
            .encode(x=f"{time_column}:T", y=f"{target_column}:Q"),
            data=hist_data,
        )
        .facet(
            row=alt.Row(
                f"{description_column}:O",
                sort="ascending",
                title=None,
                header=alt.Header(labels=False),
            ),
            spacing=60,
        )
        .resolve_scale(y="independent")
        .transform_filter(
            alt.FieldOneOfPredicate(field=f"{filter_field}", oneOf=filter_value)
        )
    )

    return st.altair_chart(bullet_chart | sparkline)


# Import data & clean up data types
df_time = create_df_from_query(
    """
    with

    query as (

        select 
            *,
            daily_minutes_target *.6 as daily_minutes_target_fail,
            daily_minutes_target *.90 as daily_minutes_target_low,
            daily_minutes_target *1.2 as daily_minutes_target_above,

            case
                when rolling_avg_daily_minutes_actual < daily_minutes_target *.6 then ' 🚩'
            end as failure_flag,

            case 
                when task_category = 'deep_work_okr' then 'Time spent in deep work on personal OKRs (6 week rolling average of minutes per day)'
                when task_category = 'deep_work_professional' then 'Time spent in deep work on professional priorities (6 week rolling average of minutes per day)'
                when task_category = 'slope_learning' then 'Time spent learning and practicing (6 week rolling average of minutes per day)'
            end as display_description

        from analytics.mart_quantified_self.ps_daily_time_tracks

        where task_category is not null and date_day >= '2020-11-01'
    )

    select
        *,

        concat(display_description, failure_flag) as display_description_with_flag
    
    from query
    """
)

kpis_time = df_time.copy()

kpis_time["date_day"] = pd.to_datetime(kpis_time["date_day"])

df_notes = create_df_from_query(
    """
    with

    query as (

        select 
            *,
            daily_notes_target *.6 as daily_notes_target_fail,
            daily_notes_target *.90 as daily_notes_target_low,
            daily_notes_target *1.2 as daily_notes_target_above,

            case
                when rolling_avg_daily_notes_actual < daily_notes_target *.6 then ' 🚩'
            end as failure_flag,

            case 
                when task_category = 'atomic_notes' then '# atomic notes added to Zettelkasten (6 week rolling average of notes per day)'
            end as display_description

        from analytics.mart_quantified_self.ps_daily_note_writes

        where task_category is not null and date_day >= '2020-11-01'
    )

    select
        *,

        concat(display_description, failure_flag) as display_description_with_flag
    
    from query
    """
)

kpis_notes = df_notes.copy()

kpis_notes["date_day"] = pd.to_datetime(kpis_notes["date_day"])

# Set viz theme
alt.themes.enable("latimes")
st.set_page_config(layout="wide")

# Define app structure and logic


def main():

    st.title("Life Metrics")
    kpis_time_latest = kpis_time[(kpis_time["date_day"] == kpis_time["date_day"].max())]
    kpis_notes_latest = kpis_notes[(kpis_notes["date_day"] == kpis_notes["date_day"].max())]

    st.header("Focus")
    graph_as_bullet_sparkline(
        pit_data=kpis_time_latest,
        hist_data=kpis_time,
        actual_column="rolling_avg_daily_minutes_actual",
        target_column="daily_minutes_target",
        above_column="daily_minutes_target_above",
        low_value_column="daily_minutes_target_low",
        failing_value_column="daily_minutes_target_fail",
        time_column="date_day",
        flagged_description_column="display_description_with_flag",
        description_column="display_description",
        filter_field="task_category",
        filter_value=["deep_work_okr", "deep_work_professional"],
    )

    st.header("Learning")
    graph_as_bullet_sparkline(
        pit_data=kpis_time_latest,
        hist_data=kpis_time,
        actual_column="rolling_avg_daily_minutes_actual",
        target_column="daily_minutes_target",
        above_column="daily_minutes_target_above",
        low_value_column="daily_minutes_target_low",
        failing_value_column="daily_minutes_target_fail",
        time_column="date_day",
        flagged_description_column="display_description_with_flag",
        description_column="display_description",
        filter_field="task_category",
        filter_value=["slope_learning"],
    )
    graph_as_bullet_sparkline(
        pit_data=kpis_notes_latest,
        hist_data=kpis_notes,
        actual_column="rolling_avg_daily_notes_actual",
        target_column="daily_notes_target",
        above_column="daily_notes_target_above",
        low_value_column="daily_notes_target_low",
        failing_value_column="daily_notes_target_fail",
        time_column="date_day",
        flagged_description_column="display_description_with_flag",
        description_column="display_description",
        filter_field="task_category",
        filter_value=["atomic_notes"],
    )


# Initialize app
if __name__ == "__main__":
    main()
