SELECT s2.id, name, subject, score
FROM scores2 s1
    UNPIVOT (
    score FOR subject IN (
        chinese_score AS 'Chinese',
        math_score AS 'Math',
        english_score AS 'English'
    )
) s2;