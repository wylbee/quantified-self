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


@st.cache
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
            metrics_okrs.*,
            concat(
            	TRUNC(dim_okrs.objective_id,0),
            	'.',
            	TRUNC(dim_okrs.key_result_id,0) 
            ) as okr_id,
            dim_okrs.objective_id,
            dim_okrs.objective_text,
            dim_okrs.key_result_text,
            concat(
            	TRUNC(dim_okrs.objective_id,0),
            	'.',
            	TRUNC(dim_okrs.key_result_id,0),
            	' - ',
            	dim_okrs.key_result_text
            ) as okr_display_text

       from mart_quantified_self.metrics_okrs

       left outer join mart_quantified_self.dim_okrs
            on metrics_okrs.key_result_id = dim_okrs.key_result_id
        
        where metrics_okrs.key_result_id != '2'

       """
)
okrs = df.copy()

okrs["date_day"] = pd.to_datetime(okrs["date_day"])

okr_text = create_df_from_query(
    """
    select
        concat(
            TRUNC(objective_id,0),
            '.',
            TRUNC(key_result_id,0) 
        ) as okr_id,
        objective_id,
        objective_text,
        key_result_id,
        key_result_text
    
    from mart_quantified_self.dim_okrs

    where key_result_id != '2'

    order by 1
    """
)

# Set viz theme
alt.themes.enable("latimes")

# Define app structure and logic

def main():

    st.title("Personal OKRs")

    okrs_latest = okrs[(okrs['date_day'] == okrs['date_day'].max())]

    bullet_chart = alt.layer(
            alt.Chart().mark_bar(
                color= '#c0b8b4',
            ).encode(
                alt.X("target_value_good_to_max:Q", scale=alt.Scale(nice=False), title=None)
            ).properties(
                height=50
            ),
            alt.Chart().mark_bar(
                color= '#a59c99'
            ).encode(
                x="target_value_average_to_good:Q"
            ),
            alt.Chart().mark_bar(
                color='#8b827f'
            ).encode(
                x="target_value_poor_to_average:Q"
            ),
            alt.Chart().mark_bar(
                color='#385B9F',
                size=7
            ).encode(
                x='metric_value:Q',
                tooltip=[
                    alt.Tooltip(
                        "key_result_value:Q",
                        title="Key Result Target"
                    ),
                    alt.Tooltip(
                        "metric_value:Q",
                        title="Actual"
                    ),
                    alt.Tooltip(
                        "target_value_poor_to_average:Q",
                        title="Poor->Average Threshold"
                    ),
                    alt.Tooltip(
                        "target_value_average_to_good:Q",
                        title="Average->Good Threshold"
                    )
                ]
            ),
            alt.Chart().mark_tick(
                color='black'
            ).encode(
                x='key_result_value:Q'
            ),
            data=okrs_latest
        ).facet(
            row=alt.Row("okr_display_text:O", sort="ascending", title=None, header=alt.Header(labelOrient='top', labelAnchor="start"))
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
                    "target_value_good_to_max:Q", 
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
                y="target_value_average_to_good:Q"
            ),
            alt.Chart().mark_area(
                 color='#8b827f'
            ).encode(
                x="date_day:T",
                y="target_value_poor_to_average:Q"
            ),
            alt.Chart().mark_line(
                color= '#385B9F'
            ).encode(
                x='date_day:T',
                y='metric_value'
            ),
            alt.Chart().mark_line(
                color='black',
                size=1
            ).encode(
                x='date_day:T',
                y='key_result_value:Q'
            ),
            data=okrs
        ).facet(
            row=alt.Row("okr_display_text:O", sort="ascending", title=None, header=alt.Header(labels=False)),
            spacing=60
        ).resolve_scale(
            y='independent'
        )
    
    st.altair_chart(bullet_chart | sparkline)

    st.table(okr_text)


# Initialize app
if __name__ == "__main__":
    main()
