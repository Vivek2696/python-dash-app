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
print(university_list.head(10))


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
                        dbc.Row(html.Div("Top 10 Faculties' Keyword-Relevant Citation", className="text-center h4")),
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


if __name__ == '__main__':
    app.run_server(debug=True)

    # Close all connections
    mysql_conn.dispose()
    mongo_conn.close()
    neo_conn.close()
