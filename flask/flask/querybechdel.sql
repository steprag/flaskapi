select distinct m.originalTitle, b.rating
from Bechdel as b
inner join Movies as m on b.movieId = m.movieId
inner join MoviesGenres as g on b.movieId = g.movieId
where m.originalTitle like "wall·%";

select m.originalTitle, b.rating
from Bechdel as b
inner join Movies as m on b.movieId = m.movieId
inner join MoviesGenres as g on b.movieId = g.movieId;

select distinct m.originalTitle, b.rating
from Bechdel as b
inner join Movies as m on b.movieId = m.movieId
where m.originalTitle like "wall·e";

select * 
from MoviesGenres as g
inner join MoviesPeople as mp on mp.movieId = g.movieId
inner join People as p on mp.personId = p.personId;

select p.primaryName, count(distinct g.genre)
from MoviesGenres as g
inner join MoviesPeople as mp on mp.movieId = g.movieId
inner join People as p on mp.personId = p.personId
group by p.primaryName
order by count(distinct g.genre) desc
;
select distinct genre
from MoviesGenres
;

select distinct p.primaryName, g.genre
from MoviesGenres as g
inner join MoviesPeople as mp on mp.movieId = g.movieId
inner join People as p on mp.personId = p.personId
where p.primaryName like 'Tim Bev%';

SELECT * FROM Movies as m
JOIN Bechdel as b ON b.movieId = m.movieId 
;

select * from People;
select * from MoviesPeople;

SELECT  P.personId, 
        P.primaryName AS Name,
        P.birthYear,
        P.deathYear,
        CASE 
            WHEN P.deathYear IS NOT NULL THEN P.deathYear - P.birthYear
            ELSE YEAR(CURRENT_DATE) - P.birthYear
        END AS Age
FROM People AS P
-- JOIN MoviesPeople as MP on MP.personId = P.personId
-- JOIN Movies as M on M.movieId = MP.movieId 
-- WHERE P.personId=%s
; -- > {"personId":1,"birthyear":1979}

select * from MoviesPeople;
SELECT 
	MP.category,
	M.originalTitle,
	MP.characters
	FROM MoviesPeople as MP
	INNER JOIN Movies as M on MP.movieid = M.movieid  
;

select 	MP.category,
		M.originalTitle
from MoviesPeople MP
inner join Movies M on MP.movieId = M.movieId 
;  -- >  {"personId":1,"birthyear":1979, "movies":['film1','film2','film3']}    
   
SELECT 	M.movieId,
		M.originalTitle,
		M.primaryTitle AS englishTitle,
		B.rating AS bechdelScore,
        M.runtimeMinutes,
		M.startYear AS Year,
		M.movieType,
		M.isAdult
FROM Movies M
JOIN Bechdel B ON B.movieId = M.movieId 
-- WHERE M.movieId=%s
;
select * from MoviesGenres;

select * from People;