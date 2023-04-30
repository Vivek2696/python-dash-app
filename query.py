import pymongo


def get_university_list_query():
    return f'''
    SELECT DISTINCT(name) from university
    ORDER BY name;
    '''


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
        MATCH(p:PUBLICATION)-[:LABEL_BY]->(k:KEYWORD) 
        WHERE k.name = '{value}' 
        RETURN p.year as Year, COUNT(DISTINCT(p.id)) as Publications_Published 
        ORDER BY Year;
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
        {"$match": {"_id": {"$gt": 0}}},
        {"$project": {"_id": 0, "Year": "$_id", "Faculties_Contributed": "$num_faculties"}}
    ]


def get_widget6_fav_keyword_query():
    return '''
    SELECT * FROM favourite_keyword;
    '''

def drop_fav_keyword_table():
    return '''
    DROP TABLE IF EXISTS `favourite_keyword`;
    '''


def create_fav_keyword_table():
    return f'''
        CREATE TABLE `favourite_keyword`(`keyword` VARCHAR(255) UNIQUE NOT NULL,INDEX(`keyword`));
        '''


def drop_add_keyword_procedure():
    return '''
    DROP PROCEDURE IF EXISTS add_keyword;
    '''


def create_add_keyword_procedure():
    return f'''
    CREATE PROCEDURE add_keyword(IN input CHAR(255))
    INSERT INTO `AcademicWorld`.`favourite_keyword` (`keyword`)
    VALUES (input);
    '''


def drop_delete_keyword_procedure():
    return '''
    DROP PROCEDURE IF EXISTS delete_keyword;
    '''


def create_delete_keyword_procedure():
    return f'''
    CREATE PROCEDURE delete_keyword(IN input CHAR(255))
    DELETE FROM `AcademicWorld`.`favourite_keyword`
    WHERE `keyword` = input;
    '''
