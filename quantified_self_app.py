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
    heatmap_actual_column=None,
    heatmap_weekly_column=None,
    heatmap_weekly_target_column=None,
):

    bullet_chart = (
        alt.layer(
            alt.Chart()
            .mark_bar(
                color="#c0b8b4",
            )
            .encode(alt.X(f"{above_column}:Q", scale=alt.Scale(nice=False), title=None))
            .properties(height=50, width=350),
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
            .properties(height=50, width=225),
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

    heatmap = (
        alt.Chart(hist_data)
        .mark_rect()
        .encode(
            x=alt.X("year_week_number:O", axis=None),
            y=alt.Y("day_of_week:O", axis=None),
            color=alt.Color(
                f"{heatmap_actual_column}",
                scale=alt.Scale(scheme="lighttealblue"),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("monthdate(date_day):T", title="Date"),
                alt.Tooltip(f"{target_column}", title="Daily Target"),
                alt.Tooltip(f"{heatmap_actual_column}", title="Daily Value"),
                alt.Tooltip(
                    f"{heatmap_weekly_target_column}", title="Week to Date Target"
                ),
                alt.Tooltip(f"{heatmap_weekly_column}", title="Week to Date Value"),
            ],
        )
        .properties(height=50, width=50)
        .facet(
            row=alt.Row(
                f"{description_column}:O",
                sort="ascending",
                title=None,
                header=alt.Header(labels=False),
            ),
            spacing=60,
        )
        .resolve_scale(color="independent")
        .transform_filter(
            alt.FieldOneOfPredicate(field=f"{filter_field}", oneOf=filter_value)
        )
    )

    return st.altair_chart(bullet_chart | sparkline | heatmap)


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
                when task_category = 'deep_work_okr' then 'Time spent in deep work on personal OKRs (2 wk avg of minutes per day)'
                when task_category = 'deep_work_professional' then 'Time spent in deep work on professional priorities (2 wk avg of minutes per day)'
                when task_category = 'slope_learning' then 'Time spent learning and practicing (2 wk avg of minutes per day)'
            end as display_description,

            concat(extract('isoyear' from date_day),extract('week' from date_day)) as year_week_number,
            extract(isodow from date_day)-1 as day_of_week

        from analytics.mart_quantified_self.ps_daily_time_tracks

        where task_category is not null and date_day >= (current_date - interval '42 days')
    )

    select
        *,

        concat(display_description, failure_flag) as display_description_with_flag
    
    from query
    """
)

kpis_time = df_time.copy()

kpis_time["date_day"] = pd.to_datetime(kpis_time["date_day"]) + pd.Timedelta('05:00:00')

#df_notes = create_df_from_query(
#    """
#    with
#
#    query as (
#
#        select 
#            *,
#            daily_notes_target *.6 as daily_notes_target_fail,
#            daily_notes_target *.90 as daily_notes_target_low,
#            daily_notes_target *1.2 as daily_notes_target_above,
#
#            case
#                when rolling_avg_daily_notes_actual < daily_notes_target *.6 then ' 🚩'
#            end as failure_flag,
#
#            case 
#                when task_category = 'atomic_notes' then '# atomic notes added to Zettelkasten (6 wk avg of notes per day)'
#            end as display_description,
#
#            concat(extract('isoyear' from date_day),extract('week' from date_day)) as year_week_number,
#            extract(isodow from date_day)-1 as day_of_week
#
#        from analytics.mart_quantified_self.ps_daily_note_writes
#
#        where task_category is not null and date_day >= (current_date - interval '42 days')
#    )
#
#    select
#        *,
#
#        concat(display_description, failure_flag) as display_description_with_flag
#    
#    from query
#    """
#)
#
#kpis_notes = df_notes.copy()
#
#kpis_notes["date_day"] = pd.to_datetime(kpis_notes["date_day"]) + pd.Timedelta('05:00:00')

df_books = create_df_from_query(
    """
    with

    query as (

        select 
            *,
            daily_books_target *.6 as daily_books_target_fail,
            daily_books_target *.90 as daily_books_target_low,
            daily_books_target *1.2 as daily_books_target_above,

            case
                when rolling_avg_daily_books_actual < daily_books_target *.6 then ' 🚩'
            end as failure_flag,

            case 
                when task_category = 'books_read' then '# books read (2 wk avg of books per day)'
            end as display_description,

            concat(extract('isoyear' from date_day),extract('week' from date_day)) as year_week_number,
            extract(isodow from date_day)-1 as day_of_week
            

        from analytics.mart_quantified_self.ps_daily_book_reads

        where task_category is not null and date_day >= (current_date - interval '42 days')
    )

    select
        *,

        concat(display_description, failure_flag) as display_description_with_flag
    
    from query
    """
)

kpis_books = df_books.copy()

kpis_books["date_day"] = pd.to_datetime(kpis_books["date_day"]) + pd.Timedelta('05:00:00')

df_health = create_df_from_query(
    """
    with

    query as (

        select 
            *,
            70 as daily_value_target_fail,
            85 as daily_value_target_low,
            100 as daily_value_target_above,

            case
                when rolling_avg_daily_value_actual < daily_value_target *.6 then ' 🚩'
            end as failure_flag,

            case 
                when metric_name = 'sleep_score' then 'Sleep score (2 wk avg of daily sleep score)'
                when metric_name = 'readiness_score' then 'Readiness score (2 wk avg of daily readiness score)'
                when metric_name = 'activity_score' then 'Activity score (2 wk avg of daily activity score)'
            end as display_description,

            concat(extract('isoyear' from date_day),extract('week' from date_day)) as year_week_number,
            extract(isodow from date_day)-1 as day_of_week
            

        from analytics.mart_quantified_self.ps_daily_health_tracks

        where metric_name is not null and date_day >= (current_date - interval '42 days')
    )

    select
        *,

        concat(display_description, failure_flag) as display_description_with_flag
    
    from query
    """
)

kpis_health = df_health.copy()

kpis_health["date_day"] = pd.to_datetime(kpis_health["date_day"]) + pd.Timedelta('05:00:00')

# Set viz theme
alt.themes.enable("latimes")
st.set_page_config(layout="wide")

# Define app structure and logic


def main():

    st.title("Life Metrics")
    kpis_time_latest = kpis_time[(kpis_time["date_day"] == kpis_time["date_day"].max())]
#    kpis_notes_latest = kpis_notes[
#        (kpis_notes["date_day"] == kpis_notes["date_day"].max())
#    ]
    kpis_books_latest = kpis_books[
        (kpis_books["date_day"] == kpis_books["date_day"].max())
    ]
    kpis_health_latest = kpis_health[
        (kpis_health["date_day"] == kpis_health["date_day"].max())
    ]
    
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
        heatmap_actual_column="daily_minutes_actual",
        heatmap_weekly_column="weekly_minutes_actual",
        heatmap_weekly_target_column="weekly_minutes_target",
    )
    graph_as_bullet_sparkline(
        pit_data=kpis_health_latest,
        hist_data=kpis_health,
        actual_column="rolling_avg_daily_value_actual",
        target_column="daily_value_target",
        above_column="daily_value_target_above",
        low_value_column="daily_value_target_low",
        failing_value_column="daily_value_target_fail",
        time_column="date_day",
        flagged_description_column="display_description_with_flag",
        description_column="display_description",
        filter_field="metric_name",
        filter_value=["readiness_score"],
        heatmap_actual_column="daily_value_actual",
        heatmap_weekly_column="avg_weekly_value_actual",
        heatmap_weekly_target_column="avg_weekly_value_target",
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
        heatmap_actual_column="daily_minutes_actual",
        heatmap_weekly_column="weekly_minutes_actual",
        heatmap_weekly_target_column="weekly_minutes_target",
    )
#    graph_as_bullet_sparkline(
#        pit_data=kpis_notes_latest,
#        hist_data=kpis_notes,
#        actual_column="rolling_avg_daily_notes_actual",
#        target_column="daily_notes_target",
#        above_column="daily_notes_target_above",
#        low_value_column="daily_notes_target_low",
#        failing_value_column="daily_notes_target_fail",
#        time_column="date_day",
#        flagged_description_column="display_description_with_flag",
#        description_column="display_description",
#        filter_field="task_category",
#        filter_value=["atomic_notes"],
#        heatmap_actual_column="daily_notes_actual",
#        heatmap_weekly_column="weekly_notes_actual",
#        heatmap_weekly_target_column="weekly_notes_target",
#    )
    graph_as_bullet_sparkline(
        pit_data=kpis_books_latest,
        hist_data=kpis_books,
        actual_column="rolling_avg_daily_books_actual",
        target_column="daily_books_target",
        above_column="daily_books_target_above",
        low_value_column="daily_books_target_low",
        failing_value_column="daily_books_target_fail",
        time_column="date_day",
        flagged_description_column="display_description_with_flag",
        description_column="display_description",
        filter_field="task_category",
        filter_value=["books_read"],
        heatmap_actual_column="daily_books_actual",
        heatmap_weekly_column="weekly_books_actual",
        heatmap_weekly_target_column="weekly_books_target",
    )

    st.header("Health & Wellness")
    graph_as_bullet_sparkline(
        pit_data=kpis_health_latest,
        hist_data=kpis_health,
        actual_column="rolling_avg_daily_value_actual",
        target_column="daily_value_target",
        above_column="daily_value_target_above",
        low_value_column="daily_value_target_low",
        failing_value_column="daily_value_target_fail",
        time_column="date_day",
        flagged_description_column="display_description_with_flag",
        description_column="display_description",
        filter_field="metric_name",
        filter_value=["sleep_score","activity_score"],
        heatmap_actual_column="daily_value_actual",
        heatmap_weekly_column="avg_weekly_value_actual",
        heatmap_weekly_target_column="avg_weekly_value_target",
    )    


# Initialize app
if __name__ == "__main__":
    main()
