import pymongo


def get_widget1_query(limit):
    return f'''
        MATCH(university:INSTITUTE)<-[:AFFILIATION_WITH]-(faculty:FACULTY)-[:INTERESTED_IN]->(keyword:KEYWORD) 
        RETURN keyword.name AS Keywords, COUNT(DISTINCT(university))
        AS Universities_Participated ORDER BY Universities_Participated DESC LIMIT {limit};
        '''


def get_widget2_query(value):
    return f'''
        SELECT F.name as Faculty, SUM(PK.score * P.num_citations) AS 'KRC_Score' FROM faculty_publication FP JOIN
        faculty F ON FP.faculty_id = F.id JOIN
        publication P ON FP.publication_id = P.id JOIN
        publication_keyword PK ON PK.publication_id = P.id JOIN
        keyword K ON PK.keyword_id = K.id
        WHERE K.name = '{value}'
        GROUP BY F.name
        ORDER BY SUM(PK.score * P.num_citations) DESC LIMIT 10;
        '''


def get_widget3_query_pipeline(value):
    return [
        {"$unwind": "$keywords"},
        {"$match": {"keywords.name": value}},
        {"$sort": {"numCitations": pymongo.DESCENDING}},
        {"$limit": 10},
        {"$project": {"_id": 0, "Publications": "$title", "score": "$keywords.score",
                      "num_citations": "$numCitations"}}
    ]


def get_widget4_img_query(value):
    return f'''
        SELECT photo_url FROM university
        WHERE name = '{value}';
        '''


def get_widget4_count_pipeline(value):
    return [
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$affiliation.name", "keywords": {"$addToSet": "$keywords.name"}}},
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$_id", "num_of_keywords": {"$sum": 1}}},
        {"$match": {"_id": value}}
    ]


def get_widget4_graph_pipeline(value):
    return [
        {"$unwind": "$keywords"},
        {"$match": {"affiliation.name": value}},
        {"$group": {"_id": "$keywords.name", "num_of_keywords": {"$sum": 1}}},
        {"$sort": {"num_of_keywords": pymongo.DESCENDING}},
        {"$limit": 10},
        {"$project": {"_id": 0, "Keywords": "$_id", "Keyword_Occurrences": "$num_of_keywords"}}
    ]


def get_widget5_publication_query(value):
    return f'''
        SELECT p.year as Year, COUNT(DISTINCT(p.id)) AS Publications_Published FROM publication_keyword AS pk
        JOIN keyword AS k ON pk.keyword_id = k.id
        JOIN publication AS p ON pk.publication_id = p.id
        WHERE k.name = "{value}"
        GROUP BY p.year
        ORDER BY year;
        '''


def get_widget5_faculty_query(value):
    return [
        {"$unwind": "$keywords"},
        {"$match": {"keywords.name": value}},
        {"$lookup": {
            "from": "publications",
            "localField": "publications",
            "foreignField": "id",
            "as": "result"
        }},
        {"$unwind": "$result"},
        {"$group": {"_id": "$result.year", "num_faculties": {"$sum": 1}}},
        {"$sort": {"_id": pymongo.ASCENDING}},
        {"$project": {"_id": 0, "Year": "$_id", "Faculties_Contributed": "$num_faculties"}}
    ]

