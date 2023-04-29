from dash import Dash, html, dash_table, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import pymongo
import mysql_utils
import mongodb_utils
import neo4j_utils
import query

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
mysql_conn = mysql_utils.MySqlDatabase()
mongo_conn = mongodb_utils.MongoDbConnection()
neo_conn = neo4j_utils.Neo4jDatabase()

# List of Universities for widget4
university_list_query = f'''
    SELECT DISTINCT(name) from university
    ORDER BY name;
    '''
university_list = mysql_conn.execute(university_list_query)

app.layout = dbc.Container(
    [
        html.Br(),
        html.H1("Academic World by Keywords", className="text-center font-family-sans-serif"),
        html.Hr(),
        html.Br(),
        dbc.Row(
            [
                # Widget1
                dbc.Col(
                    dbc.Container([
                        html.Div("Top Keywords by Universities Contribution", className='text-center h4'),
                        dcc.Graph(figure={}, id='widget1-graph'),
                        dcc.Slider(
                            id="widget1-slider",
                            min=5,
                            max=25,
                            step=5,
                            value=10,
                            marks={x: str(x) for x in [5, 10, 15, 20, 25]},
                        ),
                    ]),
                    width=6
                ),
                # Widget2
                dbc.Col(
                    dbc.Container([
                        dbc.Row(html.Div("Top 10 Faculties by Keyword-Relevant Citation", className="text-center h4")),
                        dbc.Row([
                            dbc.Col(dbc.Input(id="widget2-input",
                                              type="text",
                                              value="machine learning",
                                              placeholder="Enter Keyword"), width=6),
                            dbc.Col(dbc.Button("Search by Keywords",
                                               color="primary",
                                               className="me-1",
                                               id="widget2-button",
                                               n_clicks=0), width=6)], align='center'),
                        dbc.Row(html.Br()),
                        dbc.Row(dash_table.DataTable(id="widget2-table",
                                                     data=[],
                                                     page_size=10,
                                                     style_table={
                                                         'overflowX': 'scroll'
                                                     }))
                    ]),
                    width=6
                )
            ],
            align="center"
        ),
        dbc.Row(html.Br()),
        dbc.Row(html.Br()),
        dbc.Row(
            [
                # Widget 3
                dbc.Col(dbc.Container([
                    dbc.Row(html.Div("Top Publication by Keyword", className="text-center h4")),
                    dbc.Row(html.Br()),
                    dbc.Row([
                        dbc.Col(dbc.Input(id="widget3-input",
                                          type="text",
                                          value="data mining",
                                          placeholder="Enter Keyword"), width=6),
                        dbc.Col(dbc.Button("Search By Keywords",
                                           id="widget3-button",
                                           color="primary",
                                           className="me-1",
                                           n_clicks=0), width=6, align="center"),
                    ], align="center"),
                    dbc.Row(html.Br()),
                    dbc.Row(dcc.Graph(figure={}, id="widget3-graph"))
                ]))
            ],
            align='center'
        ),
        dbc.Row(html.Br()),
        dbc.Row(html.Br()),
        dbc.Row([
            # Widget 4
            dbc.Col(dbc.Container([
                dbc.Row(html.Div("Keywords By University", className="text-center h4")),
                dbc.Row(html.Br()),
                dbc.Row(dcc.Dropdown(id="widget4-dropdown",
                                     options=university_list['name'],
                                     value=university_list['name'].iloc[71])),
                dbc.Row(html.Br()),
                dbc.Row([
                    dbc.Col(dbc.Container([
                        dbc.Label(id='widget4-label', className='text-center h4'),
                        dbc.Card([dbc.CardImg(top=True,
                                              id='widget4-card-image',
                                              style={"max-height": "20rem"})],)
                    ]), width=6, align="center"),
                    dbc.Col(dbc.Container([
                        html.Div("Top Keywords for selected University", className="text-center h6"),
                        dcc.Graph(figure={}, id='widget4-pie-graph')
                    ]))
                ])
            ]))
        ]),
        dbc.Row(html.Br()),
        dbc.Row([
            # Widget5
            dbc.Col(dbc.Container([
                dbc.Row(html.Div("Keywords performance over time", className='text-center h4')),
                dbc.Row(html.Br()),
                dbc.Row([
                    dbc.Col(dbc.Input(id="widget5-input",
                                      type="text",
                                      value="data mining",
                                      placeholder="Enter a Keyword"), width=6),
                    dbc.Col(dbc.Button("Search By Keywords",
                                       id="widget5-button",
                                       color="primary",
                                       className="me-1",
                                       n_clicks=0), width=6, align="center")
                ], align="center"),
                dbc.Row(html.Br()),
                dbc.Tabs(
                    [
                        dbc.Tab(label="By Faculty", tab_id="widget5-tab-1", tab_style={"width": "50%"}),
                        dbc.Tab(label="By Publication", tab_id="widget5-tab-2", tab_style={"width": "50%"})
                    ],
                    id="widget5-tabs",
                    active_tab="widget5-tab-1"
                ),
                dbc.Row(dcc.Graph(figure={}, id="widget5-graph"), align="center")
            ]))
        ]),
        html.Br(),
        html.Hr(),
        html.Div(
            html.Small(u"\u00A9" + ' CS411 Final Project by vivekdp2'),
            className="text-center")
    ]
)


# Widget1 Controls (uses neo4j)
@callback(
    Output(component_id='widget1-graph', component_property='figure'),
    Input(component_id='widget1-slider', component_property='value')
)
def update_widget1(limit):
    widget1_data = neo_conn.execute(query.get_widget1_query(limit))
    return px.bar(widget1_data, x='Keywords', y='Universities_Participated')


# Widget2 Controls
@callback(
    Output(component_id='widget2-table', component_property='data'),
    Input(component_id='widget2-button', component_property='n_clicks'),
    State(component_id='widget2-input', component_property='value')
)
def update_widget2(n_clicks, value):
    widget2_data = mysql_conn.execute(query.get_widget2_query(value))
    return widget2_data.to_dict('records')


# Widget3 Controls
@callback(
    Output(component_id='widget3-graph', component_property='figure'),
    Input(component_id='widget3-button', component_property='n_clicks'),
    State(component_id='widget3-input', component_property='value')
)
def update_widget3(n_clicks, value):
    widget3_data = mongo_conn.execute_publication(query.get_widget3_query_pipeline(value))
    return px.scatter(widget3_data,
                      x='num_citations',
                      y='score',
                      color='Publications',
                      title='Scatter plot of score vs number of citations')


# Widget 4 control 1
@callback(
    Output(component_id='widget4-card-image', component_property='src'),
    Input(component_id='widget4-dropdown', component_property='value')
)
def update_widget4_img(value):
    widget4_img_url = mysql_conn.execute(query.get_widget4_img_query(value))
    return widget4_img_url['photo_url'].iloc[0]


# Widget4 control 2
@callback(
    Output(component_id='widget4-label', component_property='children'),
    Input(component_id='widget4-dropdown', component_property='value')
)
def update_widget4_count(value):
    widget4_count_data = mongo_conn.execute_faculty(query.get_widget4_count_pipeline(value))
    count = widget4_count_data['num_of_keywords'].iloc[0]
    return f'Total Keywords found: {count}'

# Widget4 control 3
@callback(
    Output(component_id='widget4-pie-graph', component_property='figure'),
    Input(component_id='widget4-dropdown', component_property='value')
)
def update_widget4_graph(value):
    widget4_graph_data = mongo_conn.execute_faculty(query.get_widget4_graph_pipeline(value))
    return px.pie(widget4_graph_data,
                  names=widget4_graph_data['Keywords'],
                  values=widget4_graph_data['Keyword_Occurrences'],
                  hole=0.3)


# Widget5 Controls
@callback(
    Output(component_id="widget5-graph", component_property="figure"),
    Input(component_id="widget5-tabs", component_property="active_tab"),
    Input(component_id="widget5-button", component_property="n_clicks"),
    Input(component_id="widget5-input", component_property="value")
)
def switch_tabs(at, n_clicks, value):
    if at == "widget5-tab-1":
        widget5_faculty_data = mongo_conn.execute_faculty(query.get_widget5_faculty_query(value))
        return px.line(widget5_faculty_data,
                       x="Year",
                       y="Faculties_Contributed",
                       markers=True)
    elif at == "widget5-tab-2":
        widget5_publication_data = mysql_conn.execute(query.get_widget5_publication_query(value))
        return px.line(widget5_publication_data,
                       x="Year",
                       y="Publications_Published",
                       markers=True)


if __name__ == '__main__':
    app.run_server(debug=True)

    # Close all connections
    mysql_conn.close()
    mongo_conn.close()
    neo_conn.close()
