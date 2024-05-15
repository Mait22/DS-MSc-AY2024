#######################
# Import libraries
import streamlit as st
import pandas as pd
import altair as alt
#import plotly.graph_objects as go
import scipy
import psycopg2
import plotly.figure_factory as ff
import plotly.express as px
import numpy as np

from pygwalker.api.streamlit import StreamlitRenderer, init_streamlit_comm

# Page configuration
st.set_page_config(
    page_title="Vanal tehnoloogial ülesõitude logide analüüsi töölaud",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

# Database connection shared resource
@st.cache_resource
def get_criteria_database_session():
    conn = psycopg2.connect(database=st.secrets.db_credentials["database"],
                            user=st.secrets.db_credentials["username"],
                            password=st.secrets.db_credentials["password"],
                            host=st.secrets.db_credentials["host_ip"],
                            port=st.secrets.db_credentials["db_port"])
    return conn

# Session state initialization 
if 'selected_lc_name_val' not in st.session_state:
    st.session_state['selected_lc_name_val'] = 'Auvere'

if 'selected_yr_val' not in st.session_state:
    st.session_state['selected_yr_val'] = '2023'

# Fetch criteria from database
def get_criteria_from_database(lc_name):
    conn = get_criteria_database_session()
    sql = f"""select * from level_crossing_logs.criteria_vals where "lc" = '{lc_name}';"""
    dat = pd.read_sql_query(sql, conn)

    return [dat.lower_crit.iloc[0], dat.higher_crit.iloc[0]]

# Update DB criteria value
def insert_criteria_to_db(low, high):

        conn = get_criteria_database_session()
        sql = f"""update level_crossing_logs.criteria_vals set 
                "lower_crit" = {st.session_state['low_value']}, 
                "higher_crit" = {st.session_state['high_value']} 
                where "lc" = '{st.session_state['selected_lc_name_val']}';"""

        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()

# Initialize state
if 'low_value' not in st.session_state:
    st.session_state['low_value'] = get_criteria_from_database(st.session_state['selected_lc_name_val'])[0]

if 'high_value' not in st.session_state:
    st.session_state['high_value'] = get_criteria_from_database(st.session_state['selected_lc_name_val'])[1]

# Pygwalker shared resource and handler
init_streamlit_comm()
@st.cache_resource
def get_pyg_renderer(df) -> "StreamlitRenderer":
    return StreamlitRenderer(df, spec="./gw_config.json", debug=False)

def make_pyg_rendered(input_df, low_value, high_value):

   input_df["Kriteerium"] = "Normaalne"
   input_df.loc[input_df["MatchedEventDuration"] < low_value, "Kriteerium"] = "Liigkiire läbimine"
   input_df.loc[input_df["MatchedEventDuration"] > high_value, "Kriteerium"] = "Liigaeglane läbimine"
    
   return get_pyg_renderer(input_df)

# Fetch data 
@st.cache_data
def get_lc_data(yr, lc_name):
    
    conn = get_criteria_database_session()

    sql = f"""select * 
              from level_crossing_logs.matched_log_data 
              where "LcName" = '{lc_name}'
              and extract(year from "MainLogEventStartTime") = {yr}
              ;"""
    
    w_data = pd.read_sql_query(sql, conn)
    w_data = w_data.drop_duplicates(subset=['MatchedEventId'])
    return w_data

# Filter session state
def update_data_filter_session_state(lc_name, yr):

    ### Session state initialization    
    if lc_name != st.session_state['selected_lc_name_val']:

        # Update query prams
        st.session_state['selected_lc_name_val'] = lc_name
        st.session_state['selected_yr_val'] = yr

        # Query previously saved criteria values
        crit_values = get_criteria_from_database(st.session_state['selected_lc_name_val'])
        st.session_state['low_value'] = crit_values[0]
        st.session_state['high_value'] = crit_values[1]

        #Keep uncommented to debug execution order
        #st.session_state['lc_data'] = get_lc_data(lc_name, yr)
    else:
        st.session_state['selected_yr_val'] = yr

    st.session_state['lc_data'] = get_lc_data(yr, lc_name)

# Initial data loading
if 'lc_data' not in st.session_state:
    st.session_state['lc_data'] = get_lc_data(st.session_state['selected_yr_val'],  
                                              st.session_state['selected_lc_name_val'])

# Sidebar
with st.sidebar:
    st.title('🏂 Vanal tehnoloogial ülesõitude logide analüüsi töölaud')

    st.markdown('#### Töölaua kirjeldus')
    with st.expander('Mida see juhtimistöölaud teeb?', expanded=False):
        st.write('''
            TO-DO
            ''')
    
    st.markdown('#### Andmete küsimine')
    with st.form("source_data_form", clear_on_submit = False):
        lc_list = ['Kulli', 'Ruusa', 'Auvere', 'Nõo', 'Irvala', 'Vägeva', 'Sangaste', 'Lehtse', 'Lagedi', 'Soldina', 'Orava', 'Mustjõe', 'Poldri', 'Kohtla', 'Elva', 'Parila', 'Kesk-kaar', 'Kuru', 'Tambre', 'Keeni', 'Näki', 
                   'Betooni', 'Rakvere', 'Aegviidu', 'Moe', 'Aiamaa', 'Põlva', 'Mullavere', 'Palupera', 'Sonda', 'Ülejõe', 'Kalme', 'Holvandi', 'Tabivere', 'Sootaga', 'Jäneda', 'Tamsalu', 
                   'Oru', 'Lemmatsi', 'Ilumetsa', 'Kiviõli', 'Kadrina', 'Kehra', 'Sompa', 'Sordi', 'Taevaskoja', 'Mägiste', 'Tapa', 'Aardla', 'Püssi', 'Kiidjärve', 'Kabala', 
                   'Kalevi', 'Tiksoja', 'Imastu', 'Puka', 'Mõneku', 'Kärkna', 'Ropka', 'Peedu', 'Aruküla', 'Raasiku']
        selected_lc_name = st.selectbox('Vali ülesõit', lc_list)

        yr_list = ['2023','2024']
        selected_yr = st.selectbox('Vali aasta:', yr_list)

        st.form_submit_button("Lae uued andmed alla", 
                              on_click=update_data_filter_session_state(selected_lc_name, 
                                                                        selected_yr 
                                                                        ))
    
    st.markdown('#### Hoiatuse kriteeriumi väärtused')
    with st.expander('Muuda kriteeriumi väärtust', expanded=True):

            with st.form("criteria_form", clear_on_submit = False):
                crit_low = st.number_input('Sisesta normaalse rongiläbimise lühim väärtus (s):', key = 'low_value')
                crit_high = st.number_input('Sisesta normaalse rongiläbimise pikim väärtus (s):', key = 'high_value')
                
                st.form_submit_button("Uuenda väärtusi", on_click=insert_criteria_to_db(crit_low, crit_high))    


# Visual elements
def make_scatterplot(input_df, low_value, high_value):

    # Matching criteria
    input_df["Kriteerium"] = "Normaalne"
    input_df.loc[input_df["MatchedEventDuration"] < low_value, "Kriteerium"] = "Liigkiire läbimine"
    input_df.loc[input_df["MatchedEventDuration"] > high_value, "Kriteerium"] = "Liigaeglane läbimine"

    fig = px.scatter(input_df, x="MainLogEventStartTime", y="MatchedEventDuration", color="Kriteerium",
                 color_discrete_map= {'Normaalne': '#27AE60',
                                      'Liigkiire läbimine': '#E74C3C',
                                      'Liigaeglane läbimine': '#29b5e8'},
                 hover_data=['MainLogEventStartTime'])
    
    fig.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=440
    )
    return fig


# Not used
def make_dist_plot(input_df, low_value, high_value):

    # Matching criteria
    input_df["Kriteerium"] = "Normaalne"
    input_df.loc[input_df["MatchedEventDuration"] < low_value, "Kriteerium"] = "Liigkiire läbimine"
    input_df.loc[input_df["MatchedEventDuration"] > high_value, "Kriteerium"] = "Liigaeglane läbimine"

    fig = ff.create_distplot([input_df.loc[input_df["Kriteerium"] == "Normaalne", "MatchedEventDuration"],
                                 input_df.loc[input_df["Kriteerium"] == "Liigkiire läbimine", "MatchedEventDuration"],
                                 input_df.loc[input_df["Kriteerium"] == "Liigaeglane läbimine", "MatchedEventDuration"]], 
                                 ['Normaalne',
                                  'Liiga kiire',
                                  'Liiga aeglane'], 
                                  bin_size=[.1, .25, .5],
                                  colors = ["#27AE60","#E74C3C","#29b5e8"])
    
    return fig

def make_signature_scatterplot(input_df, low_value, high_value):

    #Checking custom hashing
    input_df["Kriteerium"] = "Normaalne"
    input_df.loc[input_df["MatchedEventDuration"] < low_value, "Kriteerium"] = "Liigkiire läbimine"
    input_df.loc[input_df["MatchedEventDuration"] > high_value, "Kriteerium"] = "Liigaeglane läbimine"

    fig = px.scatter(input_df, x="MainLogEventStartTime", y="MatchedEventDuration", 
                     color="Kriteerium",
                     symbol = "MatchedEventSignature",
                     color_discrete_map= {'Normaalne': '#27AE60',
                                        'Liigkiire läbimine': '#E74C3C',
                                        'Liigaeglane läbimine': '#29b5e8'},
                     hover_data=['MainLogEventStartTime'])
    
    fig.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=440
    )
    return fig

# Not used
def make_signature_scatterplot_w_counts(input_df, low_value, high_value):

    scale = alt.Scale(
    domain= input_df["MatchedEventSignature"].unique(),
    range= ["#e7ba52", "#a7a7a7", "#aec7e8", "#1f77b4", "#9467bd"][0:len(input_df["MatchedEventSignature"].unique())],
    )
    color = alt.Color("MatchedEventSignature:N", scale=scale)

    brush = alt.selection_interval(encodings=["x"])
    click = alt.selection_multi(encodings=["color"])

    points = (
        alt.Chart()
        .mark_point()
        .encode(
            alt.X("MainLogEventStartTime:T", title="Kuupäev"),
            alt.Y(
                "MatchedEventDuration:Q",
                title="Rongiläbimise kiirus",
                scale=alt.Scale(domain=[0, input_df["MatchedEventDuration"].max()]),
            ),
            color=alt.condition(brush, color, alt.value("lightgray"))
        )
        .properties(width=1100, height=500)
        .add_selection(brush)
        .transform_filter(click)
    )

    # Bars by signature type
    bars = (
        alt.Chart()
        .mark_bar()
        .encode(
            x="count()",
            y="MatchedEventSignature:N",
            color=alt.condition(click, color, alt.value("lightgray")),
        )
        .transform_filter(brush)
        .properties(
            width=550,
        )
        .add_selection(click)
    )

    chart = alt.vconcat(points, bars, data=input_df, title="Sobitatud signatuuride jagunemine ajas")
    return chart


# Value display
def make_metric(input_df, input_color, low_value, high_value):

  input_df["Kriteerium"] = "Normaalne"
  input_df.loc[input_df["MatchedEventDuration"] < low_value, "Kriteerium"] = "Liigkiire läbimine"
  input_df.loc[input_df["MatchedEventDuration"] > high_value, "Kriteerium"] = "Liigaeglane läbimine"

  normal_n = np.sum(input_df["Kriteerium"] == "Normaalne")
  fast_n = np.sum(input_df["Kriteerium"] == "Liigkiire läbimine")
  slow_n = np.sum(input_df["Kriteerium"] == "Liigaeglane läbimine")

  if input_color == 'blue':
      return st.metric(label="Aeglaseid läbimisi", value=slow_n)
  elif input_color == 'green':
      return st.metric(label="Normaalseid läbimisi", value=normal_n)
  elif input_color == 'red':
      return st.metric(label="Liigkiireid läbimisi", value=fast_n)

# Circle chart
def make_donut(input_df, input_text, input_color, low_value, high_value):

  input_df["Kriteerium"] = "Normaalne"
  input_df.loc[input_df["MatchedEventDuration"] < low_value, "Kriteerium"] = "Liigkiire läbimine"
  input_df.loc[input_df["MatchedEventDuration"] > high_value, "Kriteerium"] = "Liigaeglane läbimine"

  normal_n = np.sum(input_df["Kriteerium"] == "Normaalne")
  fast_n = np.sum(input_df["Kriteerium"] == "Liigkiire läbimine")
  slow_n = np.sum(input_df["Kriteerium"] == "Liigaeglane läbimine")
    
  if input_color == 'blue':
      chart_color = ['#29b5e8', '#155F7A']
      input_response = np.round(slow_n/ np.sum(normal_n+fast_n+slow_n)*100,1)
  elif input_color == 'green':
      chart_color = ['#27AE60', '#12783D']
      input_response = np.round(normal_n/ np.sum(normal_n+fast_n+slow_n)*100,1)
  elif input_color == 'red':
      chart_color = ['#E74C3C', '#781F16']
      input_response = np.round(fast_n/ np.sum(normal_n+fast_n+slow_n)*100,1)
    
  source = pd.DataFrame({
      "Topic": ['', input_text],
      "% value": [100-input_response, input_response]
  })
  source_bg = pd.DataFrame({
      "Topic": ['', input_text],
      "% value": [100, 0]
  })
    
  plot = alt.Chart(source).mark_arc(innerRadius=45, cornerRadius=25).encode(
      theta="% value",
      color= alt.Color("Topic:N",
                      scale=alt.Scale(
                          domain=[input_text, ''],
                          range=chart_color),
                      legend=None),
  ).properties(width=130, height=130)
    
  text = plot.mark_text(align='center', color="#29b5e8", font="Lato", fontSize=22, 
                        fontWeight=700, fontStyle="italic").encode(text=alt.value(f'{input_response} %'))
  plot_bg = alt.Chart(source_bg).mark_arc(innerRadius=45, cornerRadius=20).encode(
      theta="% value",
      color= alt.Color("Topic:N",
                      scale=alt.Scale(
                          domain=[input_text, ''],
                          range=chart_color),
                      legend=None),
  ).properties(width=130, height=130)
  return plot_bg + plot + text

# Signature desc data
def make_desc_stat(input_df):

    patternd_desc_stats = input_df.groupby('MatchedEventSignature').agg({'MatchedEventDuration': ['size', 'mean', 'min', 'max']}).rename(columns={"size": "arv", 
                                                                                                                                                  "mean": "keskmine", "min": "miinimum", "max": "maksimum"}).droplevel(axis=1, level=0).reset_index()
    
    # Hack - if min and max are both zeroes, st.dataframe throws error 
    # As a quick fix solution, I set min thats equalts to 0 to new value of 0.1 and round display to whole numbers
    if patternd_desc_stats.miinimum.any() == 0:
        patternd_desc_stats.miinimum.iloc[0,] = 0.1

    return st.dataframe(input_df.groupby('MatchedEventSignature').agg({'MatchedEventDuration': ['size','mean', 'min', 'max']}).rename(columns={"size": "arv",
                                                                                                                                               "mean": "keskmine", 
                                                                                                                                               "min": "miinimum", 
                                                                                                                                               "max": "maksimum"}).droplevel(axis=1, level=0).reset_index(),
                    column_order=("MatchedEventSignature", "arv", "keskmine", "miinimum", "maksimum"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "MatchedEventSignature": st.column_config.TextColumn(
                            "Sündmuse signatuur",
                        ),
                        "esinemise arv": st.column_config.ProgressColumn(
                            "arv",
                            format="%i",
                            min_value=0,
                            max_value=max(patternd_desc_stats.arv),
                        ),
                        "keskmine": st.column_config.ProgressColumn(
                            "keskmine",
                            format="%.2f",
                            min_value=0,
                            max_value=max(patternd_desc_stats.keskmine),
                        ),
                        "miinimum": st.column_config.ProgressColumn(
                            "miinimum",
                            format="%f",
                            min_value=0,
                            max_value=max(patternd_desc_stats.miinimum),
                        ),
                        "maksimum": st.column_config.ProgressColumn(
                            "maksimum",
                            format="%f",
                            min_value=0,
                            max_value=max(patternd_desc_stats.maksimum),
                        )
                        }
                    )



#######################
# Dashboard Main Panel
col = st.columns((1, 7), gap='medium')

# Dashboard metrics pane
with col[0]:
    st.markdown("### Vaadeldav ülesõit: " + str(st.session_state['selected_lc_name_val']))
    st.markdown('#### Läbimise kiiruse sagedused')
    make_metric(st.session_state['lc_data'], 'green', st.session_state['low_value'],  st.session_state['high_value'])
    make_metric(st.session_state['lc_data'], 'red', st.session_state['low_value'],  st.session_state['high_value'])
    make_metric(st.session_state['lc_data'], 'blue', st.session_state['low_value'],  st.session_state['high_value'])

    st.markdown('#### Rongiläbimiste jagunemine')
    padding_col = st.columns((0.2, 1, 0.2))
    with padding_col[1]:
        st.write('Normaalne')
        st.altair_chart(make_donut(st.session_state['lc_data'], 'Normaalne', 'green',st.session_state['low_value'],  st.session_state['high_value']))
        st.write('Liiga kiire')
        st.altair_chart(make_donut(st.session_state['lc_data'], 'Liiga kiire', 'red',st.session_state['low_value'],  st.session_state['high_value']))
        st.write('Liiga aeglane')
        st.altair_chart(make_donut(st.session_state['lc_data'], 'Liiga aeglane', 'blue',st.session_state['low_value'],  st.session_state['high_value']))

with col[1]:
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Kiirjoonis", "🗃 Rongiläbimise signatuuride analüüs", "🗃 Ise joonistamine", "Debug"])

    with tab1:
        with st.expander('Mida need joonised näitavad', expanded=False):
            st.write('''
                TO-DO
                ''')
        st.header("Tuvastatud rongiläbimiste jaotumine ajas")
        scatterplot = make_scatterplot(st.session_state['lc_data'], st.session_state['low_value'],  st.session_state['high_value'])
        st.plotly_chart(scatterplot, use_container_width=True)

        # Turned off as the chart is too slow
        #st.plotly_chart(make_dist_plot(st.session_state['lc_data'], st.session_state['low_value'],  st.session_state['high_value']), use_container_width=True)
        
    with tab2:
        with st.expander('Mida need joonised näitavad', expanded=False):
            st.write('''
                TO-DO
                ''')
        st.header("Signatuuride jaotumine ajas")
        scatterplot = make_signature_scatterplot(st.session_state['lc_data'], st.session_state['low_value'],  st.session_state['high_value'])
        st.plotly_chart(scatterplot, use_container_width=True)

        st.markdown('#### Sobitatud sündmuse signatuurid keskmised näitajad')
        make_desc_stat(st.session_state['lc_data'])

        # Turned off as the chart is too slow
        #st.altair_chart(make_signature_scatterplot_w_counts(st.session_state['lc_data'], st.session_state['low_value'],  st.session_state['high_value']), 
        #                theme="streamlit", 
        #                use_container_width=True)

    with tab3:
        with st.expander('Mida need joonised näitavad', expanded=False):
            st.write('''
                TO-DO
                ''')
        renderer = make_pyg_rendered(st.session_state['lc_data'], st.session_state['low_value'],  st.session_state['high_value'])
        renderer.render_explore()  
            
    # Debug tab
    with tab4:
        st.write(st.session_state['low_value'])
        st.write(st.session_state['high_value'])
        st.write(st.session_state['selected_lc_name_val'])
        st.write(st.session_state['selected_yr_val'])
        st.write(st.session_state['lc_data'])