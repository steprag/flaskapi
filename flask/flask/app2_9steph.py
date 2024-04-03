import json
import math
import os
from collections import defaultdict, Counter

from flask import Flask, request, abort, jsonify
from flask_basicauth import BasicAuth
from flask_swagger_ui import get_swaggerui_blueprint

import pymysql


app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

def remove_null_fields(obj):
    return {k:v for k, v in obj.items() if v is not None}


@app.route("/movies/<int:movie_id>")
@auth.required
def movie(movie_id):
    db_conn = pymysql.connect(host="localhost"
                            , user="root"
                            ,  password='1Azertyuiop@'
                            , database="bechdel",
                            cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT
                        M.movieId,
                        M.originalTitle,
                        M.primaryTitle AS englishTitle,
                        B.rating AS bechdelScore,
                        M.runtimeMinutes,
                        M.startYear AS Year,
                        M.movieType,
                        M.isAdult
                    FROM Movies M
                    JOIN Bechdel B ON B.movieId = M.movieId 
                    WHERE M.movieId=%s""", (movie_id, ))
        movie = cursor.fetchone()
        if not movie:
            abort(404)
        movie = remove_null_fields(movie)
        #add in the film if the test is passed or not
        movie['bechdelTest'] = movie['bechdelScore'] == 3
    
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM MoviesGenres WHERE movieId=%s", (movie_id, ))
        genres = cursor.fetchall()
        movie['genres'] = [g['genre'] for g in genres]

    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT
                        P.personId,
                        P.primaryName AS name,
                        P.birthYear,
                        P.deathYear,
                        MP.job,
                        MP.category AS role
                    FROM MoviesPeople MP
                    JOIN People P on P.personId = MP.personId
                    WHERE MP.movieId=%s
        """, (movie_id, ))
        people = cursor.fetchall()
        movie['people'] = [remove_null_fields(p) for p in people]
    db_conn.close() 
    return movie 

MAX_PAGE_SIZE = 100

@app.route("/movies")
def movies():
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = int(request.args.get('include_details', 0))

    db_conn = pymysql.connect(host="localhost", 
                            user="root",
                            password='1Azertyuiop@', 
                            database="bechdel",
                            cursorclass=pymysql.cursors.DictCursor)
    #get the movies
    with db_conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                M.movieId,
                M.originalTitle,
                M.primaryTitle AS englishTitle,
                B.rating AS bechdelScore,
                M.runtimeMinutes,
                M.startYear AS year,
                M.movieType,
                M.isAdult 
            FROM Movies as M
            JOIN Bechdel B ON B.movieId = M.movieId 
            ORDER BY movieId
            LIMIT %s
            OFFSET %s
        """, (page_size, page * page_size))
        movies = cursor.fetchall()
        movie_ids = [mov['movieId'] for mov in movies]

    if include_details:
        # Get genres
        with db_conn.cursor() as cursor:
            placeholder = ','.join(['%s'] * len(movie_ids))
            cursor.execute(f"SELECT * FROM MoviesGenres WHERE movieId IN ({placeholder})",
                        movie_ids)
            genres = cursor.fetchall()
        genres_dict = defaultdict(list)
        for obj in genres:
            genres_dict[obj['movieId']].append(obj['genre'])

        # Get people
        with db_conn.cursor() as cursor:
            placeholder = ','.join(['%s'] * len(movie_ids))
            cursor.execute(f"""
                SELECT
                    MP.movieId,
                    P.personId,
                    P.primaryName AS name,
                    P.birthYear,
                    P.deathYear,
                    MP.category AS role
                FROM MoviesPeople MP
                JOIN People P on P.personId = MP.personId
                WHERE movieId IN ({placeholder})
            """, movie_ids)
            people = cursor.fetchall()
        people_dict = defaultdict(list)
        for obj in people:
            movieId = obj['movieId']
            del obj['movieId']
            people_dict[movieId].append(obj)

        # Merge genres and people into movies
        for movie in movies:
            movieId = movie['movieId']
            movie['genres'] = genres_dict[movieId]
            movie['people'] = people_dict[movieId]

    
    # Get the total movies count   
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM Movies")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)

    db_conn.close()
    return {
        'movies': movies,
        'next_page': f'/movies?page={page+1}&page_size={page_size}',
        'last_page': f'/movies?page={last_page}&page_size={page_size}',
    }

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi.yaml',
)
app.register_blueprint(swaggerui_blueprint)


@app.route("/people/<int:person_id>")
@auth.required
def person(person_id):
    db_conn = pymysql.connect(host="localhost"
                            , user="root"
                            ,  password='1Azertyuiop@'
                            , database="bechdel"
                            ,cursorclass=pymysql.cursors.DictCursor)
    #try :
    person_info = {} 
    with db_conn.cursor() as cursor:
        cursor.execute("""
                    SELECT  
                        P.primaryName AS Name,
                        P.personId, 
                        P.birthYear,
                        P.deathYear,
                        CASE 
                            WHEN P.deathYear IS NOT NULL THEN P.deathYear - P.birthYear
                            ELSE YEAR(CURRENT_DATE) - P.birthYear
                        END AS Age
                    FROM People AS P
                    WHERE P.personid=%s""", (person_id, ))
        person_info = cursor.fetchone()
        if not person_info:
            abort(404)
        person_info = remove_null_fields(person_info)

    categories = []
    films_info = []
    with db_conn.cursor() as cursor:
        cursor.execute("""
                        SELECT    
                                MP.category,
                                M.originalTitle,
                                MP.characters,
                                M.startYear AS Year
                        FROM MoviesPeople as MP
                        INNER JOIN Movies as M on MP.movieid=M.movieid 
                        WHERE MP.personid=%s
        """, (person_id, ))
        films = cursor.fetchall()
        for film in films:
                categories.append(film['category'])  # Collecte des catégories
                films_info.append({'title': film['originalTitle'], 'character': film['characters'],'Year': film['Year']})
            # Calcul de la catégorie la plus répétée
    most_common_category = Counter(categories).most_common(1)[0][0] if categories else None
    

    person_info['mostCommonCategory'] = most_common_category
    person_info['movies'] = films_info

    #finally:
    db_conn.close() 
    return person_info

@app.route("/people")
@auth.required
def persons():
    # URL parameters
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)
    include_details = bool(int(request.args.get('include_details', 0)))

    db_conn = pymysql.connect(host="localhost", user="root", password='1Azertyuiop@', database="bechdel", cursorclass=pymysql.cursors.DictCursor)

    # Get the persons
    persons_info = []
    person_categories = defaultdict(list)
    with db_conn.cursor() as cursor:
        cursor.execute("""
                        SELECT  
                            P.primaryName AS Name,
                            P.personid, 
                            P.birthYear,
                            P.deathYear,
                            CASE 
                                WHEN P.deathYear IS NOT NULL THEN P.deathYear - P.birthYear
                                ELSE YEAR(CURRENT_DATE) - P.birthYear
                            END AS Age
                        FROM People AS P
                        ORDER BY P.personID
                        LIMIT %s
                        OFFSET %s
        """, (page_size, page * page_size))
        persons = cursor.fetchall()
        if not persons:
            abort(404)
        # Extraction des ID de personnes pour la requête suivante
        person_ids = tuple([p['personid'] for p in persons])

                # Récupérer les catégories pour tous les personids
        cursor.execute("""
                        SELECT MP.personid, MP.category
                        FROM MoviesPeople MP
                        WHERE MP.personid IN %s
        """, (person_ids,))
        for row in cursor.fetchall():
            person_categories[row['personid']].append(row['category'])
        
        # Calcul de la catégorie la plus répétée pour chaque personne
        for person in persons:
            categories = Counter(person_categories[person['personid']])
            most_common_category = categories.most_common(1)[0][0] if categories else "Aucune catégorie"
            person_info = {
                'Name': person['Name'],
                'Age': person['Age'],
                'BirthYear' : person['birthYear'],
                'DeathYear' : person['deathYear'],
                'MostCommonCategory': most_common_category
            }
            persons_info.append(person_info)
            #person['deathYear'] = [remove_null_fields(person) for person in persons]
            #person_info = remove_null_fields(person_info)
    #if include_details:
        # Get movies
        #with db_conn.cursor() as cursor:
            #placeholder = ','.join(['%s'] * len(person_ids))
            #cursor.execute(f"""
                #SELECT    
                #    MP.category,
                #    M.originalTitle,
                #    MP.characters,
                #    MP.personid,
                #    M.startYear AS Year
                #    FROM MoviesPeople as MP
                #    INNER JOIN Movies as M on MP.movieid=M.movieid 
                #    WHERE personid IN ({placeholder})
            #""", (person_ids, ))
            #film = cursor.fetchall()
        #films_dict = defaultdict(list)
        #for obj in film:
            #personid = obj['personid']
            #del obj['personid']
            #films_dict[personid].append(obj)
        
        # Merge genres and people into movies
        #for person in persons:
            #personid = person['personid']
            #person['category'] = films_dict[movieId]

    if include_details == True and persons:
            person_ids = tuple([person['personid'] for person in persons])
            if person_ids:  # S'assurer que la liste/tuple n'est pas vide
                with db_conn.cursor() as cursor:
                    placeholder = ', '.join(['%s'] * len(person_ids))
                    cursor.execute(f"""
                                    SELECT  
                                        MP.personid,
                                        M.originalTitle,
                                        MP.characters,
                                        M.startYear AS Year
                                    FROM MoviesPeople MP
                                    INNER JOIN Movies M ON MP.movieid = M.movieid 
                                    WHERE MP.personid IN ({placeholder})
                    """, person_ids)
                    films = cursor.fetchall()
                
                # Associer les films à chaque personne
                films_dict = defaultdict(list)
                for film in films:
                    films_dict[film['personid']].append({
                        'title': film['originalTitle'],
                        'characters': film['characters'],
                        'year': film['Year']
                    })
                
                for person in persons:
                    person['movies'] = films_dict.get(person['personid'], [])
                    persons_info.append(person)
    else:
        persons_info = persons

    # Get the total persons count
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS total FROM People")
        total = cursor.fetchone()
        last_page = math.ceil(total['total'] / page_size)

    db_conn.close()
    return {
        'people': persons_info,
        'next_page': f'/people?page={page+1}&page_size={page_size}&include_details={int(include_details)}',
        'last_page': f'/people?page={last_page}&page_size={page_size}&include_details={int(include_details)}',
    }