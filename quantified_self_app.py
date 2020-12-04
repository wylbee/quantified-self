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


# Import data & clean up data types
df = create_df_from_query(
    """
    select 
        *,
        daily_minutes_target *.6 as daily_minutes_target_fail,
        daily_minutes_target *.8 as daily_minutes_target_low,

        case
            when rolling_avg_daily_minutes_actual < daily_minutes_target *.6 then '🚩'
        end as failure_flag,

        case 
            when task_category = 'deep_work_okr' then 'Time spent in deep work on personal OKRs (6 week rolling average of minutes per day)'
            when task_category = 'deep_work_professional' then 'Time spent in deep work on professional priorities (6 week rolling average of minutes per day)'
            when task_category = 'slope_learning' then 'Time spent learning and practicing (6 week rolling average of minutes per day)'
        end as display_description

    from analytics.dev_wbrown.ps_daily_time_tracks

    where task_category is not null and date_day >= '2020-11-01'
    """
)

kpis = df.copy()

kpis["date_day"] = pd.to_datetime(kpis["date_day"])

# Set viz theme
alt.themes.enable("latimes")

# Define app structure and logic

def main():

    st.title("Life Metrics")
    kpis_latest = kpis[(kpis['date_day'] == kpis['date_day'].max())]
    st.header("Focus")
    bullet_chart = alt.layer(
            alt.Chart().mark_bar(
                color= '#c0b8b4',
            ).encode(
                alt.X("daily_minutes_target:Q", scale=alt.Scale(nice=False), title=None)
            ).properties(
                height=50
            ),
            alt.Chart().mark_bar(
                color= '#a59c99'
            ).encode(
                x="daily_minutes_target_low:Q"
            ),
            alt.Chart().mark_bar(
                color='#8b827f'
            ).encode(
                x="daily_minutes_target_fail:Q"
            ),
            alt.Chart().mark_bar(
                color='#385B9F',
                size=7
            ).encode(
                x='rolling_avg_daily_minutes_actual:Q',
                tooltip=[
                    alt.Tooltip(
                        "daily_minutes_target:Q",
                        title="System Target"
                    ),
                    alt.Tooltip(
                        "rolling_avg_daily_minutes_actual:Q",
                        title="Actual"
                    ),
                    alt.Tooltip(
                        "daily_minutes_target_fail:Q",
                        title="Failure State Threshold"
                    ),
                    alt.Tooltip(
                        "daily_minutes_target_low:Q",
                        title="Warning Threshold"
                    )
                ]
            ),
            alt.Chart().mark_tick(
                color='black'
            ).encode(
                x='daily_minutes_target:Q'
            ),
            data=kpis_latest
        ).facet(
            row=alt.Row("display_description:O", sort="ascending", title=None, header=alt.Header(labelOrient='top', labelAnchor="start"))
        ).resolve_scale(
            x='independent'
        )
    
    sparkline = alt.layer(
            alt.Chart().mark_area(
                color='#c0b8b4'
            ).encode(
                alt.X(
                    "date_day:T", 
                    scale=alt.Scale(nice=False), 
                    title=None,
                    axis=alt.Axis(labels=False, grid=False, domain=False, ticks=False)
                ),
                alt.Y(
                    "daily_minutes_target:Q", 
                    scale=alt.Scale(nice=False), 
                    title=None,
                    axis=alt.Axis(labels=False, grid=False, domain=False, ticks=False)
                )
            ).properties(
                height=50
            ),
            alt.Chart().mark_area(
                 color='#a59c99'
            ).encode(
                x="date_day:T",
                y="daily_minutes_target_low:Q"
            ),
            alt.Chart().mark_area(
                 color='#8b827f'
            ).encode(
                x="date_day:T",
                y="daily_minutes_target_fail:Q"
            ),
            alt.Chart().mark_line(
                color= '#385B9F'
            ).encode(
                x='date_day:T',
                y='rolling_avg_daily_minutes_actual:Q'
            ),
            alt.Chart().mark_line(
                color='black',
                size=1
            ).encode(
                x='date_day:T',
                y='daily_minutes_target:Q'
            ),
            data=kpis
        ).facet(
            row=alt.Row("display_description:O", sort="ascending", title=None, header=alt.Header(labels=False)),
            spacing=60
        ).resolve_scale(
            y='independent'
        )
    
    st.altair_chart(bullet_chart | sparkline)

    st.header('Learning')

# Initialize app
if __name__ == "__main__":
    main()
