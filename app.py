from dash import Dash, html, dash_table, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import sqlalchemy
import pymongo
import neo4j

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
mysql_conn = sqlalchemy.create_engine('mysql+pymysql://root:Viraj0458@localhost:3306/academicworld')
print('SQL Connected')
mongo_conn = pymongo.MongoClient('mongodb://localhost:27017')
mongo_db = mongo_conn.academicworld
print('Mongo Connected')
neo_conn = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=('root', 'Viraj0458'))
neo4j_db = neo_conn.session(database='academicworld')
print('Neo4j Connected')

# List of Universities for widget4
university_list_query = f'''
    SELECT DISTINCT(name) from university
    ORDER BY name;
    '''
university_list = pd.read_sql(university_list_query, mysql_conn)

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
        html.Br(),
        html.Hr(),
        html.Div(
            html.Small(u"\u00A9" + ' CS411 Database System Final Project by vivekdp2'),
            className="text-center")
    ]
)


# Widget1 Controls (uses neo4j)
@callback(
    Output(component_id='widget1-graph', component_property='figure'),
    Input(component_id='widget1-slider', component_property='value')
)
def update_widget1(limit):
    widget1_query = f'''
        MATCH(university:INSTITUTE)<-[:AFFILIATION_WITH]-(faculty:FACULTY)-[:INTERESTED_IN]->(keyword:KEYWORD) 
        RETURN keyword.name AS Keywords, COUNT(DISTINCT(university))
        AS Universities_Participated ORDER BY Universities_Participated DESC LIMIT {limit};
        '''
    widget1_data = pd.DataFrame(neo4j_db.run(widget1_query).data())
    return px.bar(widget1_data, x='Keywords', y='Universities_Participated')


# Widget2 Controls
@callback(
    Output(component_id='widget2-table', component_property='data'),
    Input(component_id='widget2-button', component_property='n_clicks'),
    State(component_id='widget2-input', component_property='value')
)
def update_widget2(n_clicks, value):
    widget2_query = f'''
            SELECT F.name as Faculty, SUM(PK.score * P.num_citations) AS 'KRC_Score' FROM faculty_publication FP JOIN
            faculty F ON FP.faculty_id = F.id JOIN
            publication P ON FP.publication_id = P.id JOIN
            publication_keyword PK ON PK.publication_id = P.id JOIN
            keyword K ON PK.keyword_id = K.id
            WHERE K.name = '{value}'
            GROUP BY F.name
            ORDER BY SUM(PK.score * P.num_citations) DESC LIMIT 10;
            '''
    widget2_data = pd.read_sql(widget2_query, mysql_conn)
    return widget2_data.to_dict('records')


# Widget3 Controls
@callback(
    Output(component_id='widget3-graph', component_property='figure'),
    Input(component_id='widget3-button', component_property='n_clicks'),
    State(component_id='widget3-input', component_property='value')
)
def update_widget3(n_clicks, value):
    widget3_query = mongo_db.publications.aggregate(
        [
            {"$unwind": "$keywords"},
            {"$match": {"keywords.name": value}},
            {"$sort": {"numCitations": pymongo.DESCENDING}},
            {"$limit": 10},
            {"$project": {"_id": 0, "Publications": "$title", "score": "$keywords.score",
                          "num_citations": "$numCitations"}}
        ]
    )

    widget3_data = pd.DataFrame(list(widget3_query))
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
    widget4_img_query = f'''
        SELECT photo_url FROM university
        WHERE name = '{value}';
        '''
    widget4_img_url = pd.read_sql(widget4_img_query, mysql_conn)
    return widget4_img_url['photo_url'].iloc[0]


# Widget4 control 2
@callback(
    Output(component_id='widget4-label', component_property='children'),
    Input(component_id='widget4-dropdown', component_property='value')
)
def update_widget4_count(value):
    widget4_count_query = mongo_db.faculty.aggregate(
        [
            {"$unwind": "$keywords"},
            {"$group": {"_id": "$affiliation.name", "keywords": {"$addToSet": "$keywords.name"}}},
            {"$unwind": "$keywords"},
            {"$group": {"_id": "$_id", "num_of_keywords": {"$sum": 1}}},
            {"$match": {"_id": value}}
        ]
    )
    widget4_count_data = pd.DataFrame(list(widget4_count_query))
    count = widget4_count_data['num_of_keywords'].iloc[0]
    return f'Total Keywords found: {count}'

# Widget4 control 3
@callback(
    Output(component_id='widget4-pie-graph', component_property='figure'),
    Input(component_id='widget4-dropdown', component_property='value')
)
def update_widget4_graph(value):
    widget4_graph_query = mongo_db.faculty.aggregate(
        [
            {"$unwind": "$keywords"},
            {"$match": {"affiliation.name": value}},
            {"$group": {"_id": "$keywords.name", "num_of_keywords": {"$sum": 1}}},
            {"$sort": {"num_of_keywords": pymongo.DESCENDING}},
            {"$limit": 10},
            {"$project": {"_id": 0, "Keywords": "$_id", "Keyword_Occurrences": "$num_of_keywords"}}
        ]
    )
    widget4_graph_data = pd.DataFrame(list(widget4_graph_query))
    return px.pie(widget4_graph_data,
                  names=widget4_graph_data['Keywords'],
                  values=widget4_graph_data['Keyword_Occurrences'],
                  hole=0.3)


if __name__ == '__main__':
    app.run_server(debug=True)

    # Close all connections
    mysql_conn.dispose()
    mongo_conn.close()
    neo_conn.close()
